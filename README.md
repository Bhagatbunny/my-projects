# AWS ETL Pipeline with Apache Airflow for NSE Stock Data

This project demonstrates an ETL pipeline implemented using Apache Airflow to extract stock data from the NSE (National Stock Exchange) website, upload it to Amazon S3, process it with AWS Glue, and store the results in AWS Athena for querying. The pipeline runs on a daily schedule and performs the following tasks:

1. **Extract stock data from NSE** and upload it to an S3 bucket.
2. **Upload the ETL script** to Amazon S3, which will later be used by AWS Glue.
3. **Delete any existing processed data** from a specific folder in S3 to ensure that only the latest data is available.
4. **Run an AWS Glue ETL job** to process the stock data and writes back to S3.
5. **Wait for the new processed data** to be available in S3.
6. **Create an Athena table** for querying the processed stock data.

## Prerequisites

Before running this pipeline, ensure the following:

- **Apache Airflow** is installed and properly configured.
- An **AWS account** with the necessary permissions for S3, Glue, and Athena.
- **Python dependencies** are installed via `pip`:
  - `apache-airflow`
  - `apache-airflow-providers-amazon`
  - `boto3`
  - Any additional dependencies specific to your ETL script or check requirements.txt

## Workflow Overview

This pipeline automates the extraction, transformation, and loading (ETL) of stock portfolio data:

- **Extract Data**: The pipeline fetches stock data from the NSE website(PYTHON API) and stores it in a local CSV file.
- **Upload to S3**: The local CSV file is uploaded to an S3 bucket (`raw-stock-portfolio`).
- **Glue Job**: The ETL script processes the raw stock data using AWS Glue.
- **Athena Table**: After the data is processed, a table is created in Athena for querying the stock data.

## Pipeline Tasks

### 1. Extract NSE Stock Data

The `generate_data_task` uses a Python callable `generate_data_bucket` to scrape or fetch the stock data from the [NSE website](https://www.nseindia.com/) and load it into an S3 bucket (`raw-stock-portfolio`).

The script could use web scraping libraries like `requests` and `BeautifulSoup` or any other method to extract data from NSE.

### 2. Upload ETL Script to S3

The `upload_etl_to_s3` task uploads the Glue ETL job script to S3, so that it can be referenced by the Glue job later in the pipeline.

### 3. Delete Existing Processed Data

Before starting the processing of new data, the `delete_existing_transformed` task ensures that any previous data in the `processed-stock-data` folder is deleted.

### 4. Run AWS Glue ETL Job

The `run_glue_job` task triggers the Glue ETL job (`process_stock_data`) to transform the raw stock data into a processed format.

### 5. Wait for Processed Data

The `check_s3_data` task waits for the processed CSV file (`processed_stock_data.csv`) to be available in the `processed-stock-data` folder on S3.

### 6. Create Athena Table

The `create_athena_table` task creates an Athena external table for querying the processed data. The data is stored as a CSV file in the `processed-stock-data` folder on S3.

## DAG Structure

```python
with DAG("BHAGATH_AWS_pipeline",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:
    generate_data_task >> upload_etl_to_s3 >> delete_existing_transformed >> check_s3_data >> run_glue_job >> create_athena_table
