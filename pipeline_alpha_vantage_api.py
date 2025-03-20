import requests
import json
from google.cloud import storage
import os
from test import read_file_from_bucket
#from spark_session import get_spark
from spark_session_local import get_spark
from pyspark.sql.functions import col


#REMOVE 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Users/RHeery/Documents/GCP Key/polished-medium-448423-c5-008ffe4ca108.json"

def get_api_data():

    BUCKET_NAME = "risk_data_project"
    destination_blob_name = "response.json"
    spark = get_spark()

    file_path = 'standard/std_ref_security.parquet'
    securities_df = read_file_from_bucket(spark, BUCKET_NAME, file_path, file_format="parquet", header=True, inferSchema=True)
    securities_df.show()

    rows = []

    new_securities_df = securities_df.where(
        col("name").isNull() 
        & col("currency").isNull() 
        & col("type").isNull() 
        & col("country").isNull()
        & col("sector").isNull()
    )

    print('new_securities_df')
    new_securities_df.show()

    for row in new_securities_df.collect(): 
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={row['ticker']}&apikey=E8C3NAAOJYEWWD16"
        print(url)


        r = requests.get(url)
        data = r.json()



        if data:
            print('DATAAAAAAAAAAAAAA')
            print(data)
            rows.append(
                {
                    "ticker": data['Symbol'] ,
                    "name": data['Name'], 
                    "currency": data['Currency'],
                    "type": data["AssetType"],
                    "country": data["Country"],
                    "sector": data["Sector"]
                }
            )

        elif "Information" in data and "api rate limit" in data["Information"].lower():  
            print("API rate limit exceeded! Message:", data["Information"])  

        elif not data:
            print('Invalid ticker')

            print(data)
        
            print(row)


    api_data_df = spark.createDataFrame(rows)


    securities_df = securities_df.join(
        api_data_df, 
        "ticker", 
        how="left"
    )



    #for col_name in columns_to_update:
    #    securities_df = securities_df.withColumn(
    #        col_name,
    #        when(col(f"df_new_values.{col_name}").isNotNull(), col(f"df_new_values.{col_name}"))
    #        .otherwise(col(col_name))
    #)

    # Drop the additional columns from df_new_values
    #df = df.drop(*[col for col in df.columns if col.startswith("df_new_values")])



    # Show the DataFrame
    securities_df.show()



get_api_data()

