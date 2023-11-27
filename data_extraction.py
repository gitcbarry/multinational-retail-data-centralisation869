from database_utils import DatabaseConnector 
from data_cleaning import DataCleaning 
import pandas as pd
import tabula 

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
 
    print(df.size)
    # Join the list of dataframes returned by tabula
    df = pd.concat(dfs)  
    return df

if __name__ == '__main__':
  
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
  
  #d_ext = DataExtractor()
  df_card = d_ext.retrieve_pdf_data()
  d_clean.clean_card_data(df_card)
  db_conn.upload_to_db(df_card, "dim_card_details", local_db_engine)

