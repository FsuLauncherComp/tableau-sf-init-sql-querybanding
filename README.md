# Tableau Snowflake Checker for Initial SQL Query Tags

## Overview

This script will check all Tableau workbooks in a given directory for Snowflake Initial SQL Query Tags. It will then tag workbooks with the appropriate tag based on the results of the check.

## Requirements

- Python 3.6+
- Tableau Server Client (TSC) 0.11.0+
- Tableau Document API (TDA) 0.9.0+
- Python-dotenv 0.15.0+

## Installation

1. Clone this repository
2. Create a virtual environment using `python -m venv venv`
3. Activate the virtual environment using `.venv\Scripts\activate.bat` or `source .venv/bin/activate`
4. Install the requirements using `pip install -r requirements.txt`
5. Create a .env file in the root directory of the project and add the following variables:
    - `PAT_NAME` - Your Tableau Personal Access Token Name
    - `PAT_VALUE` - Your Tableau Personal Access Token Value
    - `SERVER_ADDRESS` - The url/address of your Tableau Server
    - `SITE_ID` - The ID of the site you want to check
    - `TABLEAU_TAG` - The tag you want to apply to workbooks missing Snowflake Initial SQL Query Tags
   

## Usage

Run the script using `python main.py`

## Disclaimer

This script is provided as-is and is not supported by Tableau or myself. Please test this script in a non-production environment before running it in production.