# Installing and importing dependencies 
pip install boto3

import boto3
import pandas as pd
import psycopg2
from botocore import UNSIGNED
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor

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

# Connecting to the db
conn = psycopg2.connect(
    database="d2b_accessment",
    user="samumghe3893",
    password="ej290wFWDo",
    host="34.89.230.185",
    port="5432"
)

# Loading the raw data into the data warehouse
orders.to_sql('orders', conn, if_exists='replace', index=False)

reviews.to_sql('reviews', conn, if_exists='replace', index=False)

shipment_deliveries.to_sql('shipments_deliveries', conn, if_exists='replace', index=False)

# Close the database connection
conn.close()