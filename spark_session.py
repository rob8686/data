from pyspark.sql import SparkSession

def get_spark():
    """
    Create and return a Spark session.
    
    :param app_name: Name of the Spark application.
    :param master: Spark master URL (default: local mode).
    :return: SparkSession object.
    """
    spark = SparkSession.builder \
        .appName('GCP') \
        .getOrCreate()
    
    return spark