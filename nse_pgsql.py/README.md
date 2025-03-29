Stock Portfolio Tracker (NSE) with PostgreSQL
Overview
This script automates fetching stock prices from Yahoo Finance (NSE), processes key financial metrics, and stores the results in a PostgreSQL database. Additionally, it saves the transformed data as a CSV file for local reference.

Workflow
Fetch Stock Data:

Uses yfinance to retrieve the last two days' stock prices.

Computes price change, percentage change, and other relevant metrics.

Introduces a 2-second delay between API calls to avoid rate limits.

Transform Data:

Adds a unique Run_ID and a timestamp for tracking.

Maps stock details (quantity, buy price) from a predefined dictionary.

Computes derived metrics like Total Value, Total Returns, One-Day Returns, and P&L Percentage.

Converts column names to lowercase for PostgreSQL compatibility.

Save & Store:

Saves the transformed data to a CSV file for local storage.

Inserts the data into a PostgreSQL table (my_stock_portfolio).

Key Components
fetch_stock_data() → Retrieves stock prices from Yahoo Finance.

transfrom_data() → Cleans, enriches, and prepares data for storage.

save_to_postgresql() → Loads the processed data into a PostgreSQL table.

Logging → Tracks execution and errors for troubleshooting.