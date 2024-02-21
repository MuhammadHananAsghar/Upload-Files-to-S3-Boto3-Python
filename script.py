import os
import boto3
from tqdm import tqdm

AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_ACCESS_KEY = "XXXXXXXXXXXXXX"
AWS_REGION = "eu-west-3"
AWS_SERVICE = "s3"
             
def upload_to_s3(local_folder, bucket_name, s3_folder):
    s3_resource = boto3.resource(AWS_SERVICE, region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    bucket = s3_resource.Bucket(bucket_name)

    # List existing objects in the S3 bucket
    existing_files = set(obj.key for obj in bucket.objects.all())

    # Iterate over files in the local folder
    for filename in os.listdir(local_folder):
        local_path = os.path.join(local_folder, filename)
        s3_key = f"{s3_folder}/{filename}"

        # Check if the file already exists in S3
        if s3_key in existing_files:
            print(f"{filename} already exists in S3. Skipping.")
        else:
            # Upload the file to S3 with a progress bar
            with tqdm(total=os.path.getsize(local_path), unit='B', unit_scale=True, desc=f'Uploading {filename}') as pbar:
                bucket.upload_file(local_path, s3_key, Callback=lambda bytes_transferred: pbar.update(bytes_transferred))

            print(f"{filename} uploaded successfully.")

if __name__ == "__main__":
    # Replace these values with your own
    local_folder_path = "/"
    s3_bucket_name = "/"
    s3_folder_path = "/"

    upload_to_s3(local_folder_path, s3_bucket_name, s3_folder_path)
