from pyspark.sql import SparkSession
import os
 
def get_spark():
    """
    Create and return a Spark session optimized for running on a GCP VM.
    """
    # Automatically use the correct service account if running on a GCP VM
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("Using VM's default service account for authentication.")
 
    spark = SparkSession.builder \
        .appName('pyspark-run-with-gcp-bucket') \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()
    
    return spark