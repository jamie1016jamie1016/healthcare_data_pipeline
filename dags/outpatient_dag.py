# data_pipeline.py

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import base64
from datetime import datetime, timedelta
import io
import zipfile
import pandas as pd


# Define variables output
OUTPUT_DIR = "/opt/airflow/dags"
DELIMITER = '|'

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def process_zip_data(ti, output_dir, delimiter):
    """Process zip data: extract the first CSV, use its name, and re-save with a specific delimiter."""
    # Retrieve the base64 encoded content from XCom
    encoded_content = ti.xcom_pull(task_ids="download_zip_file")
    
    # Decode the content back to bytes
    zip_content = base64.b64decode(encoded_content)
    
    # Wrap the bytes content in a BytesIO object
    zip_content_io = io.BytesIO(zip_content)
    with zipfile.ZipFile(zip_content_io, 'r') as zip_file:
        # Iterate over files in the ZIP archive
        for file_name in zip_file.namelist():
            if file_name.endswith('.csv'):  # Process only CSV files
                with zip_file.open(file_name) as csv_file:
                    # Read the CSV file into a DataFrame
                    df = pd.read_csv(csv_file)
                    df.to_csv("/opt/airflow/dags/123.csv", index=False, sep=delimiter)
                    


with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="A data pipeline that downloads a zip file, extracts CSV, and saves to files",
    schedule_interval=None,  # Adjust as needed
    start_date=datetime(2024, 11, 11),
    catchup=False,
) as dag:

    download_zip_file = HttpOperator(
    task_id="download_zip_file",
    method="GET",
    http_conn_id="cms_data",  # Custom connection
    endpoint="/sites/default/files/2023-04/c3d8a962-c6b8-4a59-adb5-f0495cc81fda/Outpatient.zip",
    response_filter=lambda response: base64.b64encode(response.content).decode("utf-8"),
    log_response=True,
    do_xcom_push=True,
)





  


    process_and_save_csv = PythonOperator(
        task_id="process_and_save_csv",
        python_callable=process_zip_data,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "delimiter": DELIMITER
        },
    )

    download_zip_file >> process_and_save_csv
