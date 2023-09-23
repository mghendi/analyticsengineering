# Importing dependencies 
import boto3
import pandas as pd
import psycopg2
from botocore import UNSIGNED
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine

# Initializing unsigned S3 client
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# Bucket name and directory
bucket_name = 'd2b-internal-assessment-bucket'
directory = 'orders_data/'

# Files array
files_to_download = ['orders.csv', 'reviews.csv', 'shipment_deliveries.csv']

# Looping through the list of CSVs and downloading them
for file_name in files_to_download:
    s3_file_key = directory + file_name
    local_file_name = file_name
    
    try:
        s3.download_file(bucket_name, s3_file_key, local_file_name)
        print(f'Downloaded {file_name} to {local_file_name}')
    except Exception as e:
        print(f'Error downloading {file_name}: {str(e)}')

# Extracting data from CSVs
orders = pd.read_csv('orders.csv')
reviews = pd.read_csv('reviews.csv')
shipment_deliveries = pd.read_csv('shipment_deliveries.csv')

# Cleaning the data - renaming the orders total price column to amount
orders.rename(columns = {'total_price':'amount'}, inplace = True)

# Database parameters
db_params = {
    "database": "d2b_accessment",
    "user": "samumghe3893",
    "password": "ej290wFWDo",
    "host": "34.89.230.185",
    "port": "5432"
}

# Define the three DataFrames and their corresponding table names
dataframes = [
    {"name": "orders", "data": orders},
    {"name": "reviews", "data": reviews},
    {"name": "shipment_deliveries", "data": shipment_deliveries}
]

# Create an SQLAlchemy engine
db_uri = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
engine = create_engine(db_uri)

# Loop through the DataFrames and load them into separate tables
for df_info in dataframes:
    table_name = df_info["name"]
    dataframe = df_info["data"]
    
    # Drop the table if it exists
    engine.execute(f"DROP TABLE IF EXISTS samumghe3893_staging.{table_name}")

    # Load the DataFrame into the table
    dataframe.to_sql(table_name, engine, schema="samumghe3893_staging", index=False)

# Dispose of the SQLAlchemy engine
engine.dispose()