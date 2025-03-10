#!/usr/bin/env python3
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time


#Change this to your bucket name
BUCKET_NAME = "de-zoomcamp-hw3-akshay"

#If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "gcs.json"  
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)] # Load data from Jan 2024 to June 2024
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)

# Function to download the file and return the file path
def download_file(month) -> str:
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

# Function to verify the GCS upload
def verify_gcs_upload(blob_name) -> bool:
    '''
    Verifies if the blob exists in GCS
    blob_name: str, name of the blob to verify
    Returns: bool, True if the blob exists, False otherwise
    '''
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

# Function to upload the file to GCS
def upload_to_gcs(file_path, max_retries=3) -> None:
    '''
    Uploads a file to GCS with retries
    file_path: str, path to the file to upload
    max_retries: int, number of retries before giving up
    Returns: None
    '''
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")

# Function to clean up downloaded files
def cleanup_files(file_paths) -> None:
    '''
    Deletes the downloaded files
    file_paths: list, list of file paths to delete
    Returns: None
    '''
    for file_path in file_paths:
        try:
            os.remove(file_path)
            print(f"Deleted: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

if __name__ == "__main__":
    # Download files
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # Upload files to GCS
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))
    
    # Clean up downloaded files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(cleanup_files, filter(None, file_paths))
    

    print("All files processed and verified.")