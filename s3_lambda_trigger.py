import boto3
import os

def lambda_handler(event, context):
    glue_client = boto3.client('glue')

    # Fetch the parameters from environment variables
    bucket_name_param = os.environ.get('BUCKET_NAME')
    object_key_param = os.environ.get('OBJECT_KEY')
    glue_job_name_param = os.environ.get('GLUE_JOB_NAME')

    # Extract bucket and file details from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Check if the correct file is uploaded
    if bucket_name == bucket_name_param and object_key == object_key_param:
        print(f"New file detected: {object_key} in {bucket_name}. Triggering Glue job...")

        # Start the Glue job
        response = glue_client.start_job_run(
            JobName=glue_job_name_param,
            Arguments={
                '--input_bucket': bucket_name,
                '--input_file': object_key,
            }
        )

        print("Glue Job Triggered:", response['JobRunId'])
        return {"statusCode": 200, "body": "Glue job triggered successfully"}
    else:
        print("File does not match criteria. Ignoring event.")
        return {"statusCode": 200, "body": "No action taken"}
