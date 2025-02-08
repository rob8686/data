from spark_session import get_spark
#from spark_session_local import get_spark
from google.cloud import storage

def list_buckets():
    # Initialize a GCS client
    client = storage.Client()

    # List buckets in your project
    buckets = client.list_buckets()

    print("Buckets in the project:")
    for bucket in buckets:
        print(f" - {bucket.name}")  # Python 3 f-string

def upload_file_to_bucket(bucket_name, source_file_name, destination_blob_name):
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


def read_file_from_bucket(spark, bucket_name: str, file_path: str, file_format: str = "csv", **options):
    """
    Reads a file from Google Cloud Storage into a PySpark DataFrame.

    :param spark: SparkSession instance.
    :param bucket_name: Name of the GCS bucket.
    :param file_path: Path to the file in the bucket.
    :param file_format: Format of the file (default is 'csv').
    :param options: Additional options for the reader (e.g., header=True).
    :return: PySpark DataFrame
    """
    
    # Construct GCS file path
    gcs_path = f"gs://{bucket_name}/{file_path}"

    print(gcs_path)
    
    # Read the file into a DataFrame
    df = spark.read.format(file_format).options(**options).load(gcs_path)
    
    return df


def write_file_to_bucket(spark, df, bucket_name: str, file_path: str, file_format: str = "parquet", mode: str = "overwrite", partition_by: list = None, **options):
    """
    Writes a PySpark DataFrame to a Google Cloud Storage bucket.

    :param spark: SparkSession instance.
    :param df: The PySpark DataFrame to write.
    :param bucket_name: Name of the GCS bucket.
    :param file_path: Path to the file in the bucket.
    :param file_format: Format to write the file in (default is 'parquet').
    :param mode: Mode for writing data ('overwrite', 'append', 'ignore', 'errorifexists').
    :param partition_by: List of column names to partition the data by (for formats like Parquet).
    :param options: Additional options for the writer (e.g., header=True for CSV).
    """
    
    # Construct GCS file path
    gcs_path = f"gs://{bucket_name}/{file_path}"
    
    print(f"Writing to: {gcs_path} with mode '{mode}' and partitioning by {partition_by}")
    
    # Start the write operation
    writer = df.write.format(file_format).mode(mode).options(**options)
    
    # If partitioning is specified, add partitioning to the write operation
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    # Write the DataFrame to the specified GCS bucket
    writer.save(gcs_path)

if __name__ == "__main__":


    BUCKET_NAME = "risk_data_project"
    #SOURCE_FILE = r"D:\Users\RHeery\data\test2.csv"
    #DESTINATION_BLOB = r"test2.csv"
    FILE_PATH = "test2.csv"

    list_buckets()
    #upload_file_to_bucket(BUCKET_NAME, SOURCE_FILE, DESTINATION_BLOB)

    spark = get_spark()

    df = read_file_from_bucket(spark, BUCKET_NAME, FILE_PATH, file_format="csv", header=True, inferSchema=True)
    #df = spark.read.csv(f"gs://risk_data_project/test2.csv", header=True, inferSchema=True)

    write_file_to_bucket(spark, df, bucket_name=BUCKET_NAME, file_path='standard/data.parquet', file_format='parquet', mode='overwrite')

    df.show(5)