# multinational-retail-data-centralisation

## Table of Contents 
## A description of the project
Takes data from a variety of sources programmatically and cleans them and uploads to a database (locally, but database can be specified in YAML file).
Data is extracted from the databases with the `DataExtraction` Class, with utilities for connection to the databases in `DatabaseConnector` and cleaning in  `DataCleaning` 
The database structure is then built and then queried in the .sql files
## Installation instructions
Run from the command line requires python and several packages
## Usage instructions
Run from the command line 

`$ python populate_db --data {'users', 'card_details', 'store_details', 'products', 'date_times', 'orders_table'} --database <db_local.yaml> ` 
## File structure of the project

├── README.md
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── db_local.yaml
├── populate_db.py
├── sales_data_alter.sql
└── sales_data_query.sql

## License information
