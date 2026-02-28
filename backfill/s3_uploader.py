import os
import boto3
import time
import datetime
from botocore.exceptions import ClientError
from pathlib import Path

# Load credentials from environment variables for security
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_ROOT, "data")

def upload_and_cleanup():
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME]):
        print("[ERROR] Missing AWS credentials in environment variables.")
        return

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting S3 upload cycle to bucket: {S3_BUCKET_NAME}")
    
    if not os.path.exists(DATA_DIR):
        print(f"[INFO] Data directory {DATA_DIR} does not exist. Nothing to upload.")
        return

    upload_count = 0
    delete_count = 0
    skip_count = 0
    
    now = time.time()
    # Safety: Only upload files that haven't been touched in 2 hours (ensures hourly files are finished)
    SAFETY_GAP = 2 * 3600 

    # Walk through the entire data directory recursively
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)
                
                try:
                    mtime = os.path.getmtime(local_path)
                    
                    # If the file was modified recently, skip it to avoid race conditions with collectors
                    if (now - mtime) < SAFETY_GAP:
                        skip_count += 1
                        continue
                    
                    # Create the S3 object key (maintaining the folder structure)
                    rel_path = os.path.relpath(local_path, DATA_DIR)
                    s3_key = rel_path.replace("\\", "/") 
                    
                    # Upload
                    s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)
                    print(f"[UPLOADED] {s3_key}")
                    upload_count += 1
                    
                    os.remove(local_path)
                    print(f"[DELETED LOCAL] {local_path}")
                    delete_count += 1
                        
                except FileNotFoundError:
                    pass
                except ClientError as e:
                    print(f"[ERROR] Failed to upload {local_path}: {e}")
                except Exception as e:
                    print(f"[ERROR] Unexpected error processing {local_path}: {e}")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Cycle complete. Uploaded {upload_count} files, deleted {delete_count} files locally. Skipped {skip_count} active files.")

if __name__ == "__main__":
    print(f"S3 Uploader is now in Continuous Mode (Cycle: Every 6 hours)")
    while True:
        upload_and_cleanup()
        print(f"Waiting 6 hours until next cycle...")
        time.sleep(6 * 3600)
