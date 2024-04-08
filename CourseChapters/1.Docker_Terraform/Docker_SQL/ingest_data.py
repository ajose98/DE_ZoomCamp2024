# Useful links for getting and understanding the data
# data - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# data dictionary - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

## LIBRARY IMPORTS ##
import argparse
from urllib.request import urlretrieve

import pandas as pd 
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine, inspect

## MAIN FUNCTION DEFINITION ##
def main(params):
    """ This main function reads a parquet data file, creates a connection to a postgres db
    using the user, password, host, port and db params, creates a table (table_name) in the
    database and fills it with the data in the parquet file.
    If the table already existed, its rows are dropped and it is filled with the new data.

    params:
        user (str): postgres user to make the connection to the db
        password (str): postgres password to make the connection to the db
        host (str): host of the postgres db to make the connection to the db
        port (int): port to the postgres db
        db (str): name of the database we want to connect to
        table_name (str): name of the table where the parquet data will be inserted
        url (str): url link to the parquet data file
    """
    # unpack the parameters received
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_filename = 'yellow_tripdata_2023-09.parquet'

    # get the parquet file from the url link and save it in current folder
    urlretrieve(url, parquet_filename)

    # create a SQLAlchemy connection to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read file and read the table from file
    file = pq.ParquetFile(parquet_filename)
    table = file.read()

    # convert to pandas and check data 
    df = table.to_pandas()
    df.info()

    # We need to first create the connection to our postgres database. 
    # We can feed the connection information to generate the CREATE SQL query for the specific server. 
    # SQLAlchemy supports a variety of servers.

    # generate CREATE SQL statement (DDL) from schema for validation
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    # if the table already exists, let the user know it will be re-created from scratch
    table_already_exists = False
    if inspect(engine).has_table(table_name):
        table_already_exists = True
        print(f'The table {table_name} already existed. It will be dropped and recreated.')

    # There are more than 2 millions of rows in the dataset so the data will be loaded in batches.
    # iter_batches() is used to create batches of 100,000 rows, convert them into pandas and 
    # then load it into the postgres database.

    # Insert values into the table 
    t_start = time()
    count = 0
    for batch in file.iter_batches(batch_size=100000):
        count+=1
        batch_df = batch.to_pandas()
        print(f'inserting batch {count}...')
        b_start = time()

        # empty the table if it already existed
        if table_already_exists and count == 1:
            batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        
        batch_df.to_sql(name=table_name,con=engine, if_exists='append')
        b_end = time()
        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
        
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.') 


## PARSER ARGUMENTS ##
parser = argparse.ArgumentParser(
                    description='Ingest Parquet data to Postgres')

# this if statement is used because we want to execute this file as a script
if __name__ == "__main__":
    # these are the arguments the main function will need and will be given through the command line
    parser.add_argument('--user', help='user name for postgres')       
    parser.add_argument('--password', help='password for postgres')   
    parser.add_argument('--host', help='host for postgres')   
    parser.add_argument('--port', help='port for postgres')   
    parser.add_argument('--db', help='database name for postgres')   
    parser.add_argument('--table_name', help='name of the table where we will write the results to')   
    parser.add_argument('--url', help='url of parquet file')

    args = parser.parse_args()

    # call the main function passing the arguments above
    main(args)
