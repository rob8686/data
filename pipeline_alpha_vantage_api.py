import requests
import json
from google.cloud import storage
import os
from test import read_file_from_bucket
#from spark_session import get_spark
from spark_session_local import get_spark
from pyspark.sql.functions import col, max


#REMOVE 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Users/RHeery/Documents/GCP Key/polished-medium-448423-c5-008ffe4ca108.json"

def get_api_data(ticker, function):

    fuction_mapping = {
        'SYMBOL_SEARCH':'keywords',
        'OVERVIEW': 'symbol'
        }

    url = f"https://www.alphavantage.co/query?function={function}&{fuction_mapping[function]}={ticker}&apikey=E8C3NAAOJYEWWD16"
    r = requests.get(url)
    data = r.json()

    print('URLLLLLLLLLLLLLLLLL')
    print(url)

    if "Information" in data and "api rate limit" in data["Information"].lower():  
        print("API rate limit exceeded! Message:", data["Information"])  
        return 'limit'
    else:
        return data
    
    
def get_security_row(ticker, data, default):
    row = {
        key: data.get(key, default)  # Defaults to "N/A" if the key is missing
        for key in ["Symbol", "Name", "Currency", "AssetType", "Country", "Sector"]
    }

    row['ticker'] = ticker

    return row


def update_ref_securities():

    BUCKET_NAME = "risk_data_project"
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

    new_securities_df.show()

    for row in new_securities_df.collect(): 

        data = get_api_data(row['ticker'], 'OVERVIEW')

        print('DATAAAAAAAAAAAAAAAAAAAAAAAAAAA')
        print(data)
        print()

        if data and data !='limit':

            row = get_security_row(row['ticker'], data, "N/A")
            rows.append(row)

        elif not data:
            data_search = get_api_data(row['ticker'], 'SYMBOL_SEARCH')
            best_matches =  data_search.get('bestMatches')

            print('DATA SEARCH!!!!!!!!!!!')
            print(data_search)
            print()
            print(best_matches)

            if  best_matches is not None and len(best_matches) > 0:
                ticker_search = data_search['bestMatches'][0]['1. symbol'] 
                data = get_api_data(ticker_search, 'OVERVIEW')

                print('ticker SEARCH!!!!!!!!!!!')
                print('ticker_search: ',ticker_search)
                print(data)
                print()
  
                if data and data !='limit':
                    
                    row = get_security_row(row['ticker'], data, "N/A")
                    rows.append(row)

                elif not data:
                    row = get_security_row(row['ticker'], {}, "Error")
                    rows.append(row)

            else:
                row = get_security_row(row['ticker'], {}, "Error")
                rows.append(row)



    if len(rows) > 0:
        api_data_df = spark.createDataFrame(rows)

        api_data_df.show()

        securities_df = securities_df.join(
            api_data_df, 
            "ticker", 
            how="left"
        )

        # Show the DataFrame
        securities_df.show()


def update_market_security():

    BUCKET_NAME = "risk_data_project"
    spark = get_spark()

    file_path = 'standard/std_portfolio.parquet'
    portfolio_df = read_file_from_bucket(spark, BUCKET_NAME, file_path, file_format="parquet", header=True, inferSchema=True)
    portfolio_df.show()

    current_portfolio_df = portfolio_df.groupBy('fund', 'ticker').agg(max('created_at'))
    distinct_ticker_df = current_portfolio_df.select('ticker').distinct()

    distinct_ticker_df.show()

    for row in distinct_ticker_df.collect():

        data = get_api_data(row['ticker'], 'TIME_SERIES_DAILY_ADJUSTED')
        print(data)

#update_market_security()

update_ref_securities()


