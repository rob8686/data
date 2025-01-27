print('hello world')
#import pandas 
import pyspark
from google.cloud import storage

from google.cloud import storage

def list_buckets():
    # Initialize a GCS client
    client = storage.Client()

    # List buckets in your project
    buckets = client.list_buckets()

    print("Buckets in the project:")
    for bucket in buckets:
        print(f" - {bucket.name}")  # Python 3 f-string

def upload_file(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to the bucket.
    :param bucket_name: Name of the GCS bucket
    :param source_file_name: Local file to upload
    :param destination_blob_name: Name of the object in the bucket
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

if __name__ == "__main__":
    # Replace with your bucket name and file details
    BUCKET_NAME = "risk_data_project"
    SOURCE_FILE = r"D:\Users\RHeery\data\test2.csv"
    DESTINATION_BLOB = r"test2.csv"

    list_buckets()
    #upload_file(BUCKET_NAME, SOURCE_FILE, DESTINATION_BLOB)