from database_utils import DatabaseConnector 
from data_cleaning import DataCleaning 
import pandas as pd
import tabula 
import requests, json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import json
from io import StringIO

class DataExtractor:
  '''
  Utility class to extract data from different sources
  Extracting data from .csv files, API, JSON, RDS and AWS S3 bucket

  Methods:
  -------
  read_rds_table: db_conn, table_name:
    read the data from an RDS table passing a DatabaseConnector instance returning a pandas dataframe
  retrieve_pdf_data: pdf_url
    Read pdf data from an Amazon S3 bucket and returns a pandas dataframe
  list_number_of_stores: header_dict, store_url
    Returns the number of stores to be able to extract
  retrieve_stores_data: n_stores, header_dict, store_url
    Extract store data from the API request
  extract_from_s3: s3_url
    download and extract the information from the S3 bucket returning a pandas dataframe
  retrieve_date_events_data: json_url 
    Get the data in date information in JSON format returning a pandas dataframe 


  '''
  def read_rds_table(self, db_engine, table_name):
    '''
    Read the data from an RDS table using the DatabaseConnector class and the table name
    returning a pandas dataframe
    '''
    pd_df = pd.read_sql_table(table_name, db_engine)
    return pd_df

  def retrieve_pdf_data(self, pdf_url):
    '''
    Read pdf data from an Amazon S3 bucket and returns a pandas dataframe
    '''  
    print("Getting and reading Card Details:")
    pdf_url= "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
  
    dfs = tabula.read_pdf(pdf_url, stream=True, pages='all')
    print(len(dfs))
    print(dfs[0].head(5))
 
    # Join the list of dataframes returned by tabula
    df = pd.concat(dfs)  
    return df
  
  def list_number_of_stores(self, header_dict, store_url):
    '''
    Returns the number of stores to be able to extract in retrieve_stores_data
    '''
    #header_key = "x-api-key"
    #header_val = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    #header_dict = {header_key: header_val}
    header_dict = {header_dict['header_key']: header_dict['header_val']}
    #store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores/"
    
    response = requests.get(store_url, headers=header_dict) 
    repos = response.json()
    for repo in repos:
      print(repo)

    print("Number of stores", repos["number_stores"])
    n_stores = repos["number_stores"]
    return n_stores

  def retrieve_stores_data(self, n_stores, header_dict, store_url):
    '''
    Extract store data from the API request
    '''  
    #header_key = "x-api-key"
    #header_val = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    header_dict = {header_dict['header_key']: header_dict['header_val']}
    #store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    
    response = requests.get(store_url+"1", headers=header_dict)
    repos = response.json()
   
    for repo in repos:
      print(repo)
    print("Getting store data: ")
    #n_stores = self.list_number_of_stores
    entry_all = []
    for i in range(0,n_stores):
      print("Getting store:", i, "entry")
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
    print(api_df.info())

    return api_df

  def extract_from_s3(self, s3_url):
    '''
    use boto3 package to download and extract the information from the S3 bucket
    returning a pandas DataFrame
    '''
    s3_url = "s3://data-handling-public/products.csv"
    try:
      # Boto3 code that may raise exceptions
      df = pd.read_csv(s3_url)
      print(df)
      return df

    except NoCredentialsError:
        print("AWS credentials not found. Please configure your credentials.")

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print("The specified bucket does not exist.")
        else:
            print("An error occurred:", e)

  def retrieve_date_events_data(self, json_url):            
    '''
    Get the data in date information in JSON format and return a pandas dataframe 
    '''
    #json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    df = pd.read_json(json_url)

    return df
