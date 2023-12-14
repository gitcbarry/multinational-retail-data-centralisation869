import numpy as np
import pandas as pd

class DataCleaning:
  '''
  Clean the data from the sources imported from the DataExtractor class
  Several cleaning functions specific to the data class
  One conversion function to change the product weights

  Methods:
  -------
  clean_user_data: dataframe
    Cleans the data that comes from the DataExtraction read_rds_table method
  clean_card_data: dataframe
    Clean the card user data imported from pdf DataExtraction retrieve_pdf_data
  clean_store_data: dataframe
    Cleans the data from the DataExtraction retrieve_stores_data method
  convert_product_weights: dataframe
    Convert the product weights to kg from g, oz, and ml in the products_data dataframe
  clean_products_data: dataframe
    Cleans the data that comes from the DataExtraction extract_from_s3 method
  clean_orders_data: dataframe
     Cleans the data that comes from the DataExtraction retrieve_stores_data method
  clean_date_events_data: dataframe
    Cleans the data from the DataExtraction retrieve_date_events_data method

  '''
  def clean_user_data(self, db_df):
    '''
    Clean the user data table extracted from the database
    Specifically tailored to  
    Cleans the user data: 
      NULL values (as a string) 
      errors with dates 
      incorrectly typed values 
      rows filled with the wrong information

    Parameters:
    ----------
    db_df: Pandas DataFrame
      The DataFrame to be cleaned
    '''
    #db_df.dropna(axis=0, inplace=True)
    print("Starting User Data Cleaning: \n")
    print(db_df.info())

    # Look at a summary of all the data table columns
    for name, values in db_df.items():
      print(db_df[name].value_counts())
      #print('{name}: {value}'.format(name=name, value=values[0]))

    # Rename the country code GGB to GB
    print(db_df[db_df['country_code'] == "GGB"])
    db_df.loc[db_df['country_code'] == "GGB", 'country_code'] = "GB"
 
    # Remove/Drop all the country codes that are NULL 
    print(db_df[db_df['country_code'] == "NULL"]) 
    db_df.drop(index=db_df[db_df['country_code'].str.contains('NULL')].index, inplace=True) 
    print(db_df[db_df['country_code'] == "NULL"])
   
    regex_number = '^.*$'
    regex_ph = '[0-9\(\)\+]+'
    #regex_phone = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
    regex_cc = '\w{4,}'
    regex_space = '^\w+( +\w+)*$'
    regex_no_space = '^\S+$'

    # Remove/Drop all the country codes that have 4 or more characters
    print("country_code four or more characters: \n")
    print(db_df.loc[db_df['country_code'].str.match(regex_cc)])
    db_df.drop(index=db_df[db_df['country_code'].str.match(regex_cc)].index, inplace=True) 
    print(db_df.loc[db_df['country_code'].str.match(regex_cc)])

    # Looks at the formatting of the phone numbers 
    print("Match phone numbers: \n")
    print(db_df.loc[~db_df['phone_number'].str.match(regex_ph)])
    db_df.drop(index=db_df[~db_df['phone_number'].str.match(regex_ph)].index, inplace=True)
    print(db_df.loc[~db_df['phone_number'].str.match(regex_ph)])

    print("Match phone no space: \n")
    print(db_df.loc[db_df['phone_number'].str.match(regex_no_space)])   
    print("Match phone no space: \n") 
    print(db_df.loc[~db_df['phone_number'].str.match(regex_space)])


    # Convert the dates to datetime format using 'mixed' argument handles different formatting 
    db_df['date_of_birth'] = pd.to_datetime(db_df['date_of_birth'],format='mixed') #format='%Y-%m-%d')
    db_df['join_date'] = pd.to_datetime(db_df['join_date'],format='mixed') #format='%Y-%m-%d')
    db_df.info()
    print(db_df['date_of_birth'].value_counts())
    print(db_df['join_date'].value_counts())

    for name, values in db_df.items():
      print(db_df[name].value_counts())
    print("\nEND\n")
    db_df.info()

  def clean_card_data(self, df):
    '''
    Clean the card user data imported from pdf
    Move the values in the wrong columns to the correct ones
    Remove columns wihout useful data
    Remove strings which aren't useful
    '''
    print("Starting Card Data Cleaning: \n")
 
    # Move values to correct columns    
    mask = df["card_number expiry_date"].notna()

    df.loc[mask, "card_number"]  = df.loc[mask, "card_number expiry_date"].apply(lambda x: x.split(' ')[0])
    df.loc[mask, "expiry_date"]  = df.loc[mask, "card_number expiry_date"].apply(lambda x: x.split(' ')[1])

    # Remove all rows with the string "NULL" in them 
    filter_null = "NULL"
    df_filter = df['date_payment_confirmed'].str.contains(filter_null)
    df = df[~df_filter]

    # Drop columns which weren't read correctly
    print("Removing:", 'df["card_number expiry_date"]')
    df.drop("card_number expiry_date",axis=1, inplace=True)
    
    print("Removing:", 'df["Unnamed: 0"]')
    df.drop("Unnamed: 0",axis=1, inplace=True)
    
    # Removing columns with strings like 7FL8EU9GBF
    regex_str = '[0-9A-Z]{8,}'
    df.loc[df['date_payment_confirmed'].str.match(regex_str)]
    df_filter = df['date_payment_confirmed'].str.contains(regex_str)
    df = df[~df_filter]
   
    # Set the type of the 'date_payment_confirmed' column
    df["date_payment_confirmed"] = df["date_payment_confirmed"].apply(pd.to_datetime, format='mixed', yearfirst=True)

    # Get the card numbers which have ? at the front 
    # Change to string otherwise NaN is returned
    df["card_number"] = df["card_number"].astype(str)
    df["card_number"] = df["card_number"].str.extract('(\d+)').astype(str)

    df.info()

  def clean_store_data(self, api_df):
    '''
    Cleans the data from the DataExtraction retrieve_stores_data method
    '''
    print("Cleaning data from retrieve_stores_data method")
    #print(api_df.info())
    #print(api_df["lat"][api_df["lat"].notnull()])
    api_df.drop("lat",axis=1, inplace=True)

    #Remove rows with strange strings for data
    regex_cc = '[A-Z0-9]{4,}'
    print(api_df.loc[api_df['country_code'].str.match(regex_cc)])
    df_filter = api_df['country_code'].str.match(regex_cc)
    api_df = api_df[~df_filter]

    # Replace staff numbers with digits
    api_df['staff_numbers'] = api_df['staff_numbers'].str.replace('\D+', '',regex=True)

    # Replace strings
    api_df['continent'].replace("eeEurope", "Europe", inplace=True)
    api_df['continent'].replace("eeAmerica", "America", inplace=True)

    # Change to datetime format
    api_df['opening_date'] = api_df['opening_date'].apply(pd.to_datetime, format='mixed', yearfirst=True)
    #api_df['opening_date'] = pd.to_datetime(api_df['opening_date'],format='mixed')
    #print(api_df["opening_date"].value_counts().to_string())
    api_df.info()

    return api_df

  def convert_product_weights(self, df):
    '''
    Convert the product weights to kg from g, oz, and ml
    removes the n times of the product weight
    '''
    df.dropna(inplace=True)
    #df_weight = df["weight"].str.extract(r'[\dx .]*?([\d.]+)([kgmloz]+$)')
    df_weight = df["weight"].str.extract(r'[\dx .]*?([\d.]+)([kgmloz]+).*')
    units = {'g': 1e-3, 'oz': 0.0283495, 'ml': 1e-3, 'kg': 1}
    #df_weight[1].str.lower().map(units)
    df_weight[0] = pd.to_numeric(df_weight[0])
    df_weight[0] *= df_weight[1].str.lower().map(units)
    df["weight"] = df_weight[0]
    df.rename(columns={'weight':"weight_kg"}, inplace=True)
    #df.dropna(inplace=True)

  def clean_products_data(self, df):
    '''
    Clean the products data
    ''' 
    regex_str = '[0-9A-Z]+'
    df_filter = df['category'].str.contains(regex_str)
    df[df['category'].str.contains(regex_str)]
    #df.drop(index=df[df['category'].str.match(r'[0-9A-Z]+')].index, inplace=True)
    df["removed"] = df["removed"].replace("Still_avaliable", "Still_available")
    # Convert to datetime format
    df["date_added"] = df["date_added"].apply(pd.to_datetime, format='mixed', yearfirst=True)
    # Floating point arithmetic issue some values get additional digits
    # 1.65 goes to 1.6500000000000001

  def clean_orders_data(self, db_df):
    '''
    Clean the orders data from 
      Remove columns without data
    '''
    db_df.drop("first_name",axis=1, inplace=True)
    db_df.drop("last_name",axis=1, inplace=True)
    db_df.drop("1",axis=1, inplace=True)
    # Error if not removed
    db_df.drop("level_0",axis=1, inplace=True)
    #return db_df
  
  def clean_date_events_data(self, df):
    '''
    Clean date events data

    '''
    df.drop(index=df[df['time_period'].str.contains('NULL')].index, inplace=True) 
    regex_cc = '[A-Z0-9]{6,}' 
    df.drop(index=df[df['time_period'].str.match(regex_cc)].index, inplace=True) 
    #return df
  
