import os
from typing import Tuple
import logging
import json
import datetime

from dotenv import load_dotenv
from tableaudocumentapi import Workbook, Datasource
import tableauserverclient as TSC

logging.basicConfig(
    filename="logs.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def create_tableau_session(
    pat_name: str, pat_value: str, server_address: str, site_id: str = ""
) -> TSC.Server:
    """
    Create a Tableau session
    :param pat_name: Name of personal access token
    :param pat_value: Value of personal access token
    :return: Tableau session
    """
    logger.info("Creating Tableau session")
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=pat_name, personal_access_token=pat_value, site_id=site_id
    )
    server = TSC.Server(server_address)
    server.version = "3.11"
    server.add_http_options(options_dict={"verify": False})
    server.auth.sign_in(tableau_auth)
    return server


def download_resource(
    server: TSC.Server, resource_id: str, resource_type: str = "workbook"
) -> str:
    """
    Download workbook or datasource from Tableau Server
    :param server: Tableau Server
    :param resource_id: Workbook or datasource ID
    :param source_type: Type of resource to download
    """
    logger.info(f"Downloading {resource_type} - {resource_id}")
    if resource_type not in ["workbook", "datasource"]:
        raise ValueError("source_type must be either workbook or datasource")
    if resource_type == "workbook":
        endpoint = getattr(server, "workbooks")
    else:
        endpoint = getattr(server, "datasources")

    try:
        # Download to content folder
        downloaded_resource = endpoint.download(
            resource_id, filepath="content", include_extract=False
        )
    except TSC.ServerResponseError as e:
        print(e)
        raise
    return downloaded_resource


def get_workbooks_from_config(server: TSC.Server) -> TSC.Pager:
    """
    Get workbooks less than a certain size
    :param server: Tableau Server
    :param size: Size in megabytes
    :return: Workbooks less than a certain size
    """
    with open("workbook_config.json") as f:
        config = json.load(f)
    
    size = config["content_size"]
    date_checkpoint = config["date_checkpoint"]

    size_to_bytes = size * 1024 * 1024
    logger.info(f"Getting workbooks less than {size}mb")
    req_opts = TSC.RequestOptions()
    req_opts.filter.add(
        TSC.Filter(
            "size",
            TSC.RequestOptions.Operator.LessThanOrEqual,
            size_to_bytes,
        )
    )
    req_opts.filter.add(
        TSC.Filter(
            "updatedAt",
            TSC.RequestOptions.Operator.GreaterThanOrEqual,
            date_checkpoint,
        )
    )
    workbooks = TSC.Pager(server.workbooks, request_opts=req_opts)
    return workbooks


def get_connections_for_dbclass(
    filename: str, dbclass: str = "snowflake"
) -> Tuple[str, list]:
    logger.info(f"Getting connections for {filename} with dbclass {dbclass}")
    dbclass_connections = []
    # Open the workbook or datasource file
    if filename.endswith(".twb") or filename.endswith(".twbx"):
        resource = Workbook(filename)
        datasources = resource.datasources
    elif filename.endswith(".tds") or filename.endswith(".tdsx"):
        resource = Datasource.from_file(filename)
        datasources = [resource]
    else:
        print("Unsupported file type")
        return dbclass_connections

    # Loop through all datasources
    for connection in datasources:
        ds_connections = connection.connections
        for connection in ds_connections:
            if connection.dbclass == dbclass:
                dbclass_connections.append(connection)

    return dbclass_connections


def filter_wbs_with_snowflake_connections(
    server: TSC.Server,
    workbooks: TSC.Pager,
) -> list[TSC.WorkbookItem]:
    """
    Filter workbooks with Snowflake connections
    :param workbooks: Workbooks
    :return: Workbooks with Snowflake connections
    """
    logger.info("Filtering workbooks with Snowflake connections")
    wbs_with_sf_connections = []
    for wb in workbooks:
        logger.info(f"Getting connections for workbook - {wb.name}")
        server.workbooks.populate_connections(wb)
        for connection in wb.connections:
            if connection.connection_type == "snowflake":
                logger.info(f"Found Snowflake connection in workbook - {wb.name}")
                wbs_with_sf_connections.append(wb)
                break
    return wbs_with_sf_connections


def update_workbook_config_with_date_checkpoint() -> str:
    """
    Update workbook config with date checkpoint
    """
    logger.info("Updating workbook config with date checkpoint")
    with open("workbook_config.json") as f:
        config = json.load(f)

    # Get the current date and time
    now = datetime.datetime.now()

    # Format to match 2020-12-31T00:00:00Z
    formatted_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    config["date_checkpoint"] = formatted_date

    with open("workbook_config.json", "w") as f:
        json.dump(config, f, indent=4)

    return formatted_date

def init_sql_has_query_tag(init_sql: str) -> bool:
    """
    Check if initial SQL has query tag
    :param init_sql: Initial SQL
    :return: True if initial SQL has query tag, False otherwise
    """
    query_tag_text = "ALTER SESSION SET QUERY_TAG"
    return init_sql.upper().strip().startswith(query_tag_text)

def add_workbook_tag(server: TSC.Server, wb: TSC.WorkbookItem, tag: str) -> None:
    """
    Add tag to workbook
    :param server: Tableau Server
    :param wb: Workbook
    :param tag: Tag to add
    """
    logger.info(f"Adding tag {tag} to workbook {wb.name}")
    wb.tags.add(tag)
    server.workbooks.update(wb)
    return wb.tags

def main():
    if not os.path.exists("content"):
        os.makedirs("content")

    TABLEAU_TAG = os.getenv("TABLEAU_TAG")

    # Create Tableau session
    server = create_tableau_session(
        pat_name=os.getenv("PAT_NAME"),
        pat_value=os.getenv("PAT_VALUE"),
        server_address=os.getenv("SERVER_ADDRESS"),
        site_id=os.getenv("SITE_ID"),
    )

    # Get workbooks based on config settings
    workbooks_to_check = get_workbooks_from_config(server=server)

    # Filter to workbooks with Snowflake connections
    wbs_with_sf_connections = filter_wbs_with_snowflake_connections(
        server=server, workbooks=workbooks_to_check
    )

    for wb in wbs_with_sf_connections:
        workbook_name = wb.name
        logger.info(f"Downloading workbook - {workbook_name}")
        resource_filename = download_resource(
            server=server, resource_id=wb.id, resource_type="workbook"
        )

        snowflake_connections = get_connections_for_dbclass(
            filename=resource_filename, dbclass="snowflake"
        )
        query_tag_count = 0
        for sf_connection in snowflake_connections:
            has_query_tag = init_sql_has_query_tag(sf_connection.initial_sql)
            connection_dbname = sf_connection.dbname
            if has_query_tag:
                logger.info(f"Query tag found for connection - {connection_dbname}")
                continue
            else:
                logger.info(f"Query tag not found for connection - {connection_dbname}")
                query_tag_count += 1
        if query_tag_count > 0:
            updated_wb_tags = add_workbook_tag(server=server, wb=wb, tag=TABLEAU_TAG)
            logger.info(f"Updated workbook tags - {updated_wb_tags}")

        os.remove(resource_filename)

    updated_config_date = update_workbook_config_with_date_checkpoint()
    logger.info(f"Updated config date - {updated_config_date}")

if __name__ == "__main__":
    load_dotenv()
    main()
