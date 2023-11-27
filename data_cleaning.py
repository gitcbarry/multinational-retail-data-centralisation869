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
    df = pd.concat(dfs)
    print(df.size)
    df.info()
    print(df.head(5))
    
    # Drop columns which weren't read correctly
    print(df["card_number expiry_date"])
    df = df.drop("card_number expiry_date",axis=1)
    df.head(5)
    print(df["Unnamed: 0"])
    df = df.drop("Unnamed: 0",axis=1)
    
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
