from database_utils import DatabaseConnector 
from data_cleaning import DataCleaning 
import pandas as pd
import tabula 
import requests, json

class DataExtractor:
  '''
  Utility class to extract data from different sources
  Extracting data from .csv files, API, and AWS S3 bucket

  '''
  def read_rds_table(self, db_conn, table_name):
    '''
    Read the data from an RDS table using the DatabaseConnector class and the table name
    returning a pandas dataframe
    '''
    pd_df = pd.read_sql_table(table_name, db_conn.get_engine())
    return pd_df

  def retrieve_pdf_data(self):
    '''
    Read pdf data from an Amazon S3 bucket and returns a pandas dataframe
    '''  
    print("Getting and reading Card Details:")
    pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
  
    dfs = tabula.read_pdf(pdf_path, stream=True, pages='all')
    print(len(dfs))
    print(dfs[0].head(5))
 
    # Join the list of dataframes returned by tabula
    df = pd.concat(dfs)  
    return df
  
  def list_number_of_stores(self):
    '''
    Returns the number of stores to extract
    '''
    header_key = "x-api-key"
    header_val = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    header_dict = {header_key: header_val}

    n_stores_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores/"
    
    response = requests.get(n_stores_url, headers=header_dict) 
    repos = response.json()
    for repo in repos:
      print(repo)

    print("Number of stores", repos["number_stores"])
    n_stores = repos["number_stores"]
    return n_stores

  def retrieve_stores_data(self, n_stores):
    '''
    Extract store data
    '''  
    header_key = "x-api-key"
    header_val = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    header_dict = {header_key: header_val}
    store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"

    response = requests.get(store_url+"1", headers=header_dict)
    repos = response.json()
   
    for repo in repos:
      print(repo)

    #n_stores = self.list_number_of_stores
    entry_all = []
    for i in range(1,n_stores):
      response = requests.get(store_url+str(i), headers=header_dict)
      repos = response.json()
      entry_list = []
      for repo in repos:
        entry = repos[repo]
        entry_list.append(entry)
      entry_all.append(entry_list)
      #print("Index:", repos["index"], "\n")
    
    #print(entry_all) 
    # More efficient to make the dataframe here rather than in the loop
    api_df = pd.DataFrame(entry_all, columns=repos)
    print("dataframe \n")
    print(api_df.head(5))
    print(api_df.info())


if __name__ == '__main__':
  ''' 
  db_conn = DatabaseConnector()
  yaml_creds = db_conn.read_db_creds("db_creds.yaml")
  engine = db_conn.init_db_engine(yaml_creds)
  table_list = db_conn.list_db_tables(engine)

  print(table_list[1])
  d_ext = DataExtractor()
  db_df = d_ext.read_rds_table(db_conn, table_list[1])
  print(db_df.head(5))
  print(db_df.info())

  d_clean = DataCleaning()
  d_clean.clean_user_data(db_df)

  local_creds = db_conn.read_db_creds("db_local.yaml")
  local_db_engine = db_conn.init_db_engine(local_creds)
  db_conn.upload_to_db(db_df, "dim_users", local_db_engine)
  

  df_card = d_ext.retrieve_pdf_data()
  d_clean.clean_card_data(df_card)
  db_conn.upload_to_db(df_card, "dim_card_details", local_db_engine)
  '''
  d_ext = DataExtractor()
  n_stores = d_ext.list_number_of_stores()
  api_df = d_ext.retrieve_stores_data(n_stores)

  d_clean = DataCleaning()
  d_clean.called_clean_store_data(api_df)

  db_conn = DatabaseConnector()
  local_creds = db_conn.read_db_creds("db_local.yaml")
  local_db_engine = db_conn.init_db_engine(local_creds)
  db_conn.upload_to_db(api_df, "dim_store_details", local_db_engine)



  
