import numpy as np
import pandas as pd

class DataCleaning:
  '''
  Clean the data from the sources imported from the DataExtractor class
  
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
    print("Starting Data Cleaning: \n")
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

    #db_df.loc[db_df['phone_number'].str.match(regex_number), 'error'] = np.nan
    #db_df.loc[~db_df['phone_number'].str.match(regex_phone), 'error'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
    #db_df.loc[~db_df['country_code'].str.match(regex_cc), 'error'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN

    for name, values in db_df.items():
      print(db_df[name].value_counts())
    print("\nEND\n")
    db_df.info()

  def clean_card_data(self, df):
    '''
    Clean the card user data imported from pdf
    '''
    print("Starting Card Data Cleaning: \n")
  
    # Concatenate the list of DataFrames
   
    print(df.size)
    df.info()
    print(df.head(5))
    
    # Drop columns which weren't read correctly
    print(df["card_number expiry_date"])
    df.drop("card_number expiry_date",axis=1, inplace=True)
    
    print(df["Unnamed: 0"])
    df.drop("Unnamed: 0",axis=1, inplace=True)
    
    # Remove all rows with the string "NULL" in them 
    print(df[df["card_provider"] == "NULL"])
    df.drop(index=df[df['card_provider'].str.contains('NULL')].index, inplace=True) 
    
    # Replace rows which contain card providers which aren't a name
    regex_cc = '[A-Z0-9]{6,}'    
    print(df.loc[df['card_provider'].str.match(regex_cc)])
    df.drop(index=df[df['card_provider'].str.match(regex_cc)].index, inplace=True) 
    df['expiry_date'].value_counts(dropna=False)
   
    # Set the type
    df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'],format='mixed')
    df.info()
    df.head()
    df[df["expiry_date"].isnull()]
    df.dropna(inplace=True)
    df.info()
    # Change to a numeric type 
    df['card_number'] = pd.to_numeric(df['card_number'], errors="coerce", downcast="integer")
    #df[df["card_number"] < 1e10]
    # Drop the NaN values
    df.dropna(inplace=True)

    df.info()

  def called_clean_store_data(self, api_df):
    '''
    Cleans the data from the DataExtraction retrieve_stores_data method
    '''
    print("Cleaning data from retrieve_stores_data method")
    print(api_df.info())
    print(api_df["lat"][api_df["lat"].notnull()])
    api_df.drop("lat",axis=1, inplace=True)

    regex_cc = '[A-Z0-9]{4,}'
    print(api_df.loc[api_df['country_code'].str.match(regex_cc)])
    api_df.drop(index=api_df[api_df['country_code'].str.match(regex_cc)].index, inplace=True)
    
    api_df['continent'].replace("eeEurope", "Europe", inplace=True)
    api_df['continent'].replace("eeAmerica", "America", inplace=True)

    api_df['opening_date'] = pd.to_datetime(api_df['opening_date'],format='mixed')
    print(api_df["opening_date"].value_counts().to_string())
    api_df.info()

    return api_df

  def convert_product_weights(self, df):
    '''
    Convert the product weights to kg 
    removes the n times of the product weight
    '''
    df.dropna(inplace=True)
    df_weight = df["weight"].str.extract(r'[\dx .]*?([\d.]+)([kgmloz]+$)')
    units = {'g': 1e-3, 'oz': 0.0283495, 'ml': 1e-3, 'kg': 1}
    #df_weight[1].str.lower().map(units)
    df_weight[0] = pd.to_numeric(df_weight[0])
    df_weight[0] *= df_weight[1].str.lower().map(units)
    df["weight"] = df_weight[0]
    df.rename(columns={'weight':"weight_kg"}, inplace=True)
    df.dropna(inplace=True)


  def clean_products_data(self,df):
    '''
    Clean the products data
    ''' 
    df.drop(index=df[df['category'].str.match(r'[0-9A-Z]+')].index, inplace=True)
    df["removed"] = df["removed"].replace("Still_avaliable", "Still_available")
    # Convert to datetime format
    df["date_added"] = pd.to_datetime(df['date_added'],format='mixed')  
    # Floating point arithmetic issue some values get additional digits
    # 1.65 goes to 1.6500000000000001

  def clean_orders_data(self, db_df):
    '''
    Clean the orders 
    '''
    db_df.drop("first_name",axis=1, inplace=True)
    db_df.drop("last_name",axis=1, inplace=True)
    db_df.drop("1",axis=1, inplace=True)
    # Error if not removed
    db_df.drop("level_0",axis=1, inplace=True)
    return db_df
  
  def clean_date_events_data(self, df):
    '''
    Clean date events data
    '''
    df.drop(index=df[df['time_period'].str.contains('NULL')].index, inplace=True) 
    regex_cc = '[A-Z0-9]{6,}' 
    df.drop(index=df[df['time_period'].str.match(regex_cc)].index, inplace=True) 
  
