import boto3
from botocore import UNSIGNED
from botocore.client import Config

id = "samumghe3893" 
bucket_name = "d2b-internal-assessment-bucket"
s3_folder = f"analytics_export/{id}"

# Initialize the S3 client
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# List of CSV files to upload
csv_files = ["agg_public_holiday.csv", "agg_shipments.csv", "best_performing_product.csv"]

# Upload each CSV file to S3
for file_name in csv_files:
    s3_key = f"{s3_folder}/{file_name}"
    local_file_path = file_name
    
    try:
        # Upload the file to S3
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Uploaded {file_name} to S3: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {str(e)}")