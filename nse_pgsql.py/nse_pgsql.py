import uuid
import yfinance as yf
import pandas as pd
import time
import os
import sys
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import FileUtils
from my_stocks import MyStockData
from connect_to_pgsql import get_db_connection

# Load environment variables
load_dotenv()

# Logger Configuration
logger = FileUtils.set_logger("nse_pgsql")

# PostgreSQL Connection
engine = get_db_connection()

def fetch_stock_data(my_stocks):
    """Fetch raw stock data from yfinance (NSE)"""
    stock_data = []
    
    for stock in my_stocks.keys():
        try:
            nse_symbol = f"{stock}.NS"  # Format for NSE in yfinance
            print(f"Fetching data for {nse_symbol}...")
            logger.info(f"Fetching data for {nse_symbol}...")
            
            ticker = yf.Ticker(nse_symbol)
            hist = ticker.history(period="2d")  # Get last two days' prices

            if not hist.empty:
                last_close = hist["Close"].iloc[-1]  # Last closing price
                prev_close = hist["Close"].iloc[-2] if len(hist) > 1 else last_close
                change = last_close - prev_close
                change_percent = (change / prev_close) * 100 if prev_close else 0

                stock_info = {
                    "Stock": stock,
                    "Last_Price": round(last_close, 2),
                    "Change": round(change, 2),
                    "Change_percent": round(change_percent, 2)
                }
                
                stock_data.append(stock_info)
            else:
                logger.warning(f"No historical data for {stock}")
            
            time.sleep(2)  # Added 2-second delay
        except Exception as e:
            logger.error(f"Error fetching {stock}: {e}")
    
    return pd.DataFrame(stock_data)

def transfrom_data(df, my_stocks):
    
    if df.empty:
        logger.warning("No data to manipulate.")
        return df

    run_id = str(uuid.uuid4())  # Generate a unique Run ID
    current_timestamp = pd.Timestamp.now()  # Creates Current Timestamp

    df["Run_ID"] = run_id
    df["Timestamp"] = current_timestamp
    df["Quantity"] = df["Stock"].map(lambda stock: my_stocks[stock]["quantity"])
    df["Buy_Price"] = df["Stock"].map(lambda stock: my_stocks[stock]["buy_price"])
    
    df["Total_Value"] = df["Quantity"] * df["Last_Price"]
    df["Total_Returns"] = (df["Last_Price"] - df["Buy_Price"]) * df["Quantity"]
    df["One_Day_Returns"] = df["Change"] * df["Quantity"]
    df["PL_Percentage"] = (df["Total_Returns"] / (df["Quantity"] * df["Last_Price"])) * 100
    df["PL_Percentage"] = df["PL_Percentage"].round(2)

    # df = df[df["Total_Returns"] > 0]
    df.columns = df.columns.str.lower() 

    local_path= os.getenv("LOCAL_PATH")
    file_path = f"{local_path}/my_stocks_data.csv"
    df.to_csv(file_path, index=False)

    logger.info(f"Data transformation completed. Run ID: {run_id} and Saved df to path:{local_path} ")
    return df

def save_to_postgresql(df):
    """Save the DataFrame to PostgreSQL"""
    try:
        if not df.empty:
            table_name = os.getenv("TABLE_NAME")
            Schema = os.getenv("SCHEMA")
            df.to_sql(table_name, engine, if_exists="append", index=False, schema=Schema)
            logger.info("Data successfully inserted into PostgreSQL.")
        else:
            logger.warning("No data to insert.")
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")

def main():
    """Main function to fetch, manipulate, and store data"""
    my_stocks = MyStockData.my_stocks()
    
    df = fetch_stock_data(my_stocks)
    df = transfrom_data(df, my_stocks)
    save_to_postgresql(df)
    
if __name__ == "__main__":
    main()