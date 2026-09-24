import requests
import json 
import time
import datetime
import boto3


api_url = "https://www.alphavantage.co/query"
api_key = "ALPHA_VANTAGE_API_KEY"
TICKERS = ["NVDA", "AAPL", "GOOGL", "MSFT", "AMZN"]
s3_bucket = "us-stock-analysis-pipeline"

def lambda_handler(event, context) : 

    #initialize s3
    s3_client = boto3.client('s3')

    for ticker in TICKERS:
        parameters = {
            "function" : "TIME_SERIES_DAILY",
            "symbol": ticker,
            "apikey": api_key
        }

        response = requests.get(api_url, params=parameters)
        raw_data = response.json()

        if "Information" in raw_data or "Error Message" in raw_data:
            print(f"{ticker} has retrival failed")
            continue
        else :
            
            time_series = raw_data[ "Time Series (Daily)"]
            clean_records = []

            for dates, prices in time_series.items() :
                row = {
                    "symbol" : ticker,
                    "date"   : dates,
                    "open": prices["1. open"],
                    "high": prices["2. high"],
                    "low": prices["3. low"],
                    "close": prices["4. close"],
                    "volume": prices["5. volume"]
                }
                clean_records.append(row)

            date_ = datetime.date.today().strftime("%Y-%m-%d")
            file_name = f"{ticker}_raw_{date_}.json"
            s3_key = f"landing/{file_name}"

            print(f"{ticker} file created and sending to s3.....")

            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body = json.dumps(clean_records)
            )

            time.sleep(13)


    # Lambda functions must return a status response
    return {
        'statusCode': 200,
        'body': json.dumps('Data ingestion completed successfully!')
    }

if __name__ == "__main__":
    print("Testing Lambda locally...")
    lambda_handler({}, {})