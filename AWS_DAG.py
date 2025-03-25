from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from nse_data_to_s3 import generate_data_bucket
from datetime import datetime

# Define variables
S3_BUCKET = "raw-stock-portfolio"
LOCAL_CSV_FILE = "C:/Users/bhagh/Documents/raw-stock-data.csv"
UPLOAD_ETL_JOB_SCRIPT = "C:/Users/bhagh/Documents/project-aws-v/my-aws-project/process_stock_data.py"
GLUE_JOB_NAME = "process_stock_data"
ATHENA_DB = "tradingdb"
ATHENA_TABLE = "my_portfolio_data"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

default_args = {
    "owner": "bhagath yennam",
    "start_date": days_ago(0),
    "retries": 1
}

# Initialize the DAG
with DAG("BHAGATH_AWS_pipeline",
         default_args=default_args,
         description='Fetch NSE stock data and upload to S3',
         schedule_interval="@daily",
         catchup=False) as dag:

    # 1. Generate NSE stock data and land it into S3
    generate_data_task = PythonOperator(
        task_id="load_nse_data_s3",
        python_callable=generate_data_bucket
    )

    # 2. Upload ETL local Glue script to S3
    upload_etl_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_etl_to_s3",
        filename=UPLOAD_ETL_JOB_SCRIPT,
        dest_key="Glue-etl-scripts/process_stock_data.py",
        dest_bucket=S3_BUCKET,
        replace=True
    )

    # 3. Delete existing CSV file in transformed-folder ensuring only one CSV exists to load to Athena
    delete_existing_transformed = BashOperator(
        task_id="delete_existing_processed_data",
        bash_command="aws s3 rm s3://raw-stock-portfolio/processed-stock-data/ --recursive"
    )

    # 4. Run AWS Glue ETL Job
    run_glue_job = GlueJobOperator(
        task_id="run_etl_glue_job",
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True
    )

    # 5. Wait for new processed data in S3
    check_s3_data = S3KeySensor(
        task_id="check_new_processed_s3_data",
        bucket_name=S3_BUCKET,
        bucket_key="processed-stock-data/processed_stock_data.csv",
        timeout=300,
        poke_interval=60,
        mode="poke"
    )

    # 6. Create Athena table
    create_athena_table = AthenaOperator(
        task_id="create_athena_table",
        query=f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE} (
            stock STRING,
            last_price DOUBLE,
            change DOUBLE,
            change_percent DOUBLE,
            quantity INT,
            total_value DOUBLE,
            total_returns DOUBLE,
            one_day_returns DOUBLE,
            load_timestamp STRING
        )
        STORED AS CSV
        LOCATION 's3://{S3_BUCKET}/processed-stock-data/';
        """,
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT
    )

    # Define task dependencies
    generate_data_task >> upload_etl_to_s3 >> delete_existing_transformed >> check_s3_data >> run_glue_job >> create_athena_table
