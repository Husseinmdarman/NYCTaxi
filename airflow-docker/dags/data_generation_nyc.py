from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import boto3
import os
import time

def classify_response_status(status_code: int) -> str:
    """
    Classify CloudFront response codes for NYC Taxi dataset.

    Returns:
        "ok"      → file exists
        "retry"   → temporary issue, retry recommended
        "missing" → file not published yet
    """
    if status_code == 200:
        return "ok"

    # Rate limit or temporary server errors
    if status_code in (429, 500, 502, 503, 504):
        return "retry"

    # NYC TLC uses 403 AccessDenied for missing files
    if status_code in (403, 404):
        return "missing"

    return "missing"
    

def download_nyc_taxi_data(execution_date, **context):
    year = execution_date.year
    month = execution_date.month

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

    output_dir = "/Data"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/yellow_tripdata_{year}-{month:02d}.parquet"

    # Skip if already downloaded
    if os.path.exists(output_path):
        print(f"File {output_path} already exists. Skipping download.")
        return

    max_retries = 5
    backoff = 5

    for attempt in range(1, max_retries + 1):
        response = requests.get(url)
        status = classify_response_status(response.status_code)

        if status == "ok":
            break

        if status == "missing":
            print(f"File not published yet for {year}-{month:02d}. Skipping.")
            return

        if status == "retry":
            print(f"Attempt {attempt}: temporary issue ({response.status_code}). Retrying...")
            time.sleep(backoff * attempt)
            continue

    # If still not OK after retries
    if classify_response_status(response.status_code) != "ok":
        print(f"Failed after {max_retries} retries. Skipping {year}-{month:02d}.")
        return

    # Write file
    with open(output_path, "wb") as f:
        f.write(response.content)

    print(f"Downloaded file to {output_path}")


def upload_to_s3(execution_date, **context):
    
    print("DEBUG: upload task started") 
    print("DEBUG: cwd:", os.getcwd()) 
    print("DEBUG: /Data exists:", os.path.exists("/Data")) 
    print("DEBUG: /root/.aws exists:", os.path.exists("/root/.aws"))
    year = execution_date.year
    month = execution_date.month

    output_path = f"/Data/yellow_tripdata_{year}-{month:02d}.parquet"

    # Skip if file missing (e.g., download skipped)
    if not os.path.exists(output_path):
        print(f"Local file {output_path} does not exist. Skipping upload.")
        return

    s3 = boto3.client("s3", region_name="eu-west-2")
    bucket_name = "historic-taxi-data"
    s3_key = f"yellow_tripdata/{year}/yellow_tripdata_{year}-{month:02d}.parquet"
    print("Contents of /Data:", os.listdir("/Data"))
    print("Looking for:", output_path)
    s3.upload_file(output_path, bucket_name, s3_key)
    print(f"Uploaded {output_path} to s3://{bucket_name}/{s3_key}")


with DAG(
    dag_id="yellow_taxi_data_monthly_ingestions",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True,
) as dag:

    download_task = PythonOperator(
        task_id="download_nyc_taxi_data",
        python_callable=download_nyc_taxi_data,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    download_task >> upload_task

