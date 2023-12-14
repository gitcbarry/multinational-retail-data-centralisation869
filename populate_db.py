import database_utils as du
import data_cleaning as dc
import data_extraction as dx
import argparse


if __name__ == '__main__':
  choices_list = ['users','card_details','store_details','products','date_times','orders_table']
  parser = argparse.ArgumentParser(description='Get, clean and upload to local database')
  parser.add_argument('-d','--data', help='Data to get, clean and upload', choices=choices_list, required=True)
  parser.add_argument('-db','--database', help='Database YAML file path', required=True)
  args = vars(parser.parse_args())

  # Initialise the classes 
  d_clean = dc.DataCleaning()
  db_conn = du.DatabaseConnector()
  d_ext = dx.DataExtractor()

  yaml_creds = db_conn.read_db_creds("db_creds.yaml")
  db_engine = db_conn.init_db_engine(yaml_creds)

  local_creds = db_conn.read_db_creds(args['database'])
  local_db_engine = db_conn.init_db_engine(local_creds)

  def get_user_data():
    table_list = db_conn.list_db_tables(db_engine)
    print(table_list[1])
    db_df = d_ext.read_rds_table(db_engine, table_list[1])
    print(db_df.head(5))
    print(db_df.info())
    d_clean.clean_user_data(db_df)
    db_conn.upload_to_db(db_df, "dim_users", local_db_engine)

  def get_card_data():  
    df_card = d_ext.retrieve_pdf_data()
    d_clean.clean_card_data(df_card)
    db_conn.upload_to_db(df_card, "dim_card_details", local_db_engine)

  def get_store_data():
    store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores/"
    api_dict = db_conn.read_db_creds("api_key.yaml")
    n_stores = d_ext.list_number_of_stores(api_dict,store_url)
    api_df = d_ext.retrieve_stores_data(n_stores,api_dict,store_url)
    d_clean.clean_store_data(api_df)
    db_conn.upload_to_db(api_df, "dim_store_details", local_db_engine)

  def get_product_data():
    s3_df = d_ext.extract_from_s3()
    d_clean.convert_product_weights(s3_df)
    d_clean.clean_products_data(s3_df)
    db_conn.upload_to_db(s3_df, "dim_products", local_db_engine)

  def get_date_data():
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    df = d_ext.retrieve_date_events_data(json_url)
    d_clean.clean_date_events_data(df)
    db_conn.upload_to_db(df, "dim_date_times", local_db_engine)  

  def get_orders_data():
    table_list = db_conn.list_db_tables(db_engine)
    db_df = d_ext.read_rds_table(db_conn, table_list[2])
    print("Cleaning Data")
    d_clean.clean_orders_data(db_df)
    db_conn.upload_to_db(db_df, "orders_table", local_db_engine)

  if args['data'] == 'users':
    print("Getting cleaning and uploading", choices_list[0], "data")
    get_user_data()
    
  if args['data'] == 'card_details':
   print("Getting cleaning and uploading", choices_list[1], "data")
   get_card_data()

  if args['data'] == 'store_details':
    print("Getting cleaning and uploading", choices_list[2], "data")
    get_store_data()

  if args['data'] == 'products':
    print("Getting cleaning and uploading", choices_list[3], "data")
    get_product_data()

  if args['data'] == 'date_times':
    print("Getting cleaning and uploading", choices_list[4], "data")
    get_date_data()

  if args['data'] == 'orders_table':  
    print("Getting cleaning and uploading", choices_list[5], "data")
    get_orders_data()  

 

 









