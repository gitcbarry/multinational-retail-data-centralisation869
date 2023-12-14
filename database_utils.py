import yaml 
from sqlalchemy import create_engine
from sqlalchemy import inspect

import psycopg2

class DatabaseConnector:
  '''
  Connect and upload data to a database using sqlalchemy and reading credentials from a YAML file
  Attributes:
  ----------
    
  Methods:
  -------
    read_db_creds 
      Read the YAML credentials file and return a dictionary
    init_db_engine(yaml_dict)
      Initalise a database engine from theh YAML credentials returned form read_db_creds
    list_db_tables: database engine
      List the tables available in the database 
    upload_to_db: dataframe, table_name, engine
      Upload the pandas dataframe to a database specified in the engine with a particular table name
  '''

  def read_db_creds(self, yaml_file):
    '''
    Read the YAML credentials file and return a dictionary
    '''
    with open(yaml_file, 'r') as f:
      yaml_creds = yaml.safe_load(f)
    
    return yaml_creds
  
  def init_db_engine(self, yaml_creds):
    '''
    Takes the credentials from the `read_db_creds` method and initalises and 
    returns a sqlalchemy database engine
    '''
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2' #Probably not required
    db_string = f"{DATABASE_TYPE}+{DBAPI}://{yaml_creds['RDS_USER']}:{yaml_creds['RDS_PASSWORD']}@{yaml_creds['RDS_HOST']}:{yaml_creds['RDS_PORT']}/{yaml_creds['RDS_DATABASE']}"
    print (db_string)
    db_engine = create_engine(db_string)
    return db_engine
  
  def list_db_tables(self, db_engine):
    '''
    Lists all the tables in the database to extract data from and returns the list
    '''
    db_table_list = []
    db_column_dict = {}
    
    inspector = inspect(db_engine)

    for table_name in inspector.get_table_names():
      db_table_list.append(table_name)
      #print("Table: %s" % table_name)
      for column in inspector.get_columns(table_name):
        db_column_dict.setdefault(table_name, []).append(column['name']) 
        #print("Column: %s" % column['name'])

    print(db_table_list)
    print(db_column_dict)
    self.column_dict = db_column_dict
    return db_table_list    
 
  def upload_to_db(self, pd_df, table_name, engine):
    '''
    Takes a pandas data frame and a table name as arguments to upload to a data base
    '''
    pd_df.to_sql(name=table_name, con=engine, if_exists='replace')
