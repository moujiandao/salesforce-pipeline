import boto3
import os
from pathlib import Path

def upload_to_s3(local_file, bucket_name, s3_key):
    """Upload a file to S3"""
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(local_file, bucket_name, s3_key)
        print(f"✓ Successfully uploaded {local_file} to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        return False

def upload_all_salesforce_files(data_folder='data', bucket_name='brian-sf-pipeline-dev'):
    """Upload all CSV files from data folder to S3"""
    
    # Find all CSV files in the data folder
    data_path = Path(data_folder)
    csv_files = list(data_path.glob('*.csv'))
    
    if not csv_files:
        print("No CSV files found in data folder")
        return
    
    print(f"Found {len(csv_files)} files to upload")
    
    for csv_file in csv_files:
        # Create S3 key based on filename
        filename = csv_file.name
        s3_key = f'raw/salesforce/{filename}'
        
        # Upload
        upload_to_s3(
            local_file=str(csv_file),
            bucket_name=bucket_name,
            s3_key=s3_key
        )

if __name__ == "__main__":
    upload_all_salesforce_files(
        data_folder='data',
        bucket_name='brian-sf-pipeline-dev'
    )