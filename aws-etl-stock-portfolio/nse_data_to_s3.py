import time
import boto3
import pandas as pd
from nsetools import Nse
from io import StringIO

def fetch_stock_data(my_stocks):
    stock_data = []
    nse = Nse()
    for stock, info in my_stocks.items():
        try:
            print(f"Fetching data for {stock}...")
            quote = nse.get_quote(stock)  # Fetch stock data
            last_price = quote.get("lastPrice", 0)
            price_change = quote.get("change", 0)
            percentage_change = quote.get("pChange", 0)
            
            quantity = info["quantity"]
            buy_price = info["buy_price"]
            total_value = quantity * last_price
            total_returns = (last_price - buy_price) * quantity
            one_day_returns = price_change * quantity
            
            stock_info = {
                "Stock": stock,
                "Last_Price": last_price,
                "Change": price_change,
                "Change_%": percentage_change,
                "Quantity": quantity,
                "Total_Value": total_value,
                "Total_Returns": total_returns,
                "1D_Returns": one_day_returns
            }
            
            stock_data.append(stock_info)
            
            time.sleep(1) #API blocking prevention
        except Exception as e:
            print(f"Error fetching {stock}: {e}")
    return pd.DataFrame(stock_data)

def upload_to_s3(bucket_name, s3_file_path, df):
    try:
        s3_client = boto3.client("s3")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=s3_file_path)
        print(f"File uploaded to S3: s3://{bucket_name}/{s3_file_path}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def generate_data_bucket():
    my_stocks = {
        "IOC": {"quantity": 800, "buy_price": 127.65},
        "CANBK": {"quantity": 510, "buy_price": 100.41},
        "TRIDENT": {"quantity": 100, "buy_price": 27.3},
        "EMIL": {"quantity": 3, "buy_price": 130.36},
        "TATAMOTORS": {"quantity": 65, "buy_price": 782.60},
        "GTLINFRA": {"quantity": 518, "buy_price": 1.66}
    }
    
    df = fetch_stock_data(my_stocks)
    
    # S3 Upload
    s3_bucket_name = "raw-stock-portfolio"
    s3_file_path = "raw-stock-data.csv"
    
    upload_to_s3(s3_bucket_name, s3_file_path, df)

if __name__ == "__main__":
    generate_data_bucket()
