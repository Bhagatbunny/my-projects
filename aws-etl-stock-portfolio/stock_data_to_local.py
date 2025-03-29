from nsetools import Nse
import pandas as pd
import time
import os
import sys
import boto3
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import FileUtils

load_dotenv()
nse = Nse()

# Logger Configuration
logger = FileUtils.set_logger("nas_s3")

def fetch_stock_data(my_stocks):
    stock_data = []
    for stock, info in my_stocks.items():
        try:
            print(f"Fetching data for {stock}...")
            logger.info(f"Fetching data for {stock}...")
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
            
            logger.info(f"Fetched Data: {stock_info}")
            stock_data.append(stock_info)
            
            time.sleep(3)  # delay to prevent API blocking
        except Exception as e:
            logger.error(f"Error fetching {stock}: {e}")
    return pd.DataFrame(stock_data)

def upload_to_s3(filename):
    try:
        session = boto3.Session()
        s3_client = session.client("s3")
        
        S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        S3_FILE_PATH = os.getenv("S3_FILE_PATH")
        
        s3_client.upload_file(filename, S3_BUCKET_NAME, S3_FILE_PATH)
        logger.info(f"File uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")

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
    LOCAL_PATH = os.getenv("LOCAL_PATH")
    filename = f"{LOCAL_PATH}/raw-stock-data.csv"
    FileUtils.check_and_create(LOCAL_PATH, logger)
    df.to_csv(filename, index=False)
    logger.info(f"File saved to NAS at {filename}")
    upload_to_s3(filename)
    
if __name__ == "__main__":
    generate_data_bucket()
