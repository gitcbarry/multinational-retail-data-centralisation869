import numpy as np

class DataCleaning:
  '''
  Clean the data from the sources imported from the DataExtractor class
  '''
  def clean_user_data(self, db_df):
    '''
    Clean the user data table extracted from the data_base
    '''
    #db_df.dropna(axis=0, inplace=True)
    print("Starting Data Cleaning: \n")
    print(db_df.info())
    #print(db_df['country'].value_counts())

    print(db_df[db_df['country_code'] == "GGB"])
    #db_df = db_df.drop(db_df[db_df['country'] == "NULL"])

    #for column in db_df:
    #  print(db_df[column]) 


    for name, values in db_df.items():
      print(db_df[name].value_counts())
      #print('{name}: {value}'.format(name=name, value=values[0]))
    print(db_df[db_df['country_code'] == "NULL"])
    print(db_df.loc[db_df['country_code'] == "NULL"])
    print(db_df[db_df['country_code'] == "5D74J6FPFJ"])
    regex_number = '^.*$'
    regex_phone = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
    #regex_cc = '^[A-Z]{2}$'
    db_df.loc[db_df['phone_number'].str.match(regex_number), 'error'] = np.nan
    #db_df.loc[~db_df['phone_number'].str.match(regex_phone), 'error'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
    #db_df.loc[~db_df['country_code'].str.match(regex_cc), 'error'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN

    #print(db_df.head(10))
    print(db_df[db_df.isna().any(axis=1)])
    print("\nEND\n")

    #db_df = db_df.drop(db_df[db_df['country'] == "NULL"])
    #for col in db_conn.get_column_dict()[table_list[1]]:
     # print(db_df[col].value_counts())
    #pd_df.dropna(axis=0, inplace=True)