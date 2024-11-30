# data_pipeline.py

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import base64
from datetime import datetime, timedelta
import io
import zipfile
import pandas as pd
from airflow.models import Connection
from airflow import settings
import subprocess


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
                    df = pd.read_csv(csv_file, sep=delimiter)
                    output_file_path = f"{output_dir}/processed_{file_name}"
                    df.to_csv(output_file_path, index=False)

def run_csv_to_db_script():
    """Run the CSV to DB script."""
    try:
        subprocess.run(
            ["python3", "/opt/airflow/scripts/csv_to_db_stage.py"],
            check=True,
        )
        print("CSV to DB script executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running CSV to DB script: {e}")

def create_http_connection():
    """Creates the HTTP connection programmatically in Airflow."""
    conn_id = "cms_data"
    session = settings.Session()  # Initialize session here
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="http",
            host="https://data.cms.gov",
        )
        session.add(new_conn)
        session.commit()
        session.close()
        print(f"Connection {conn_id} created.")
    else:
        print(f"Connection {conn_id} already exists.")
        session.close()

create_http_connection()

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
    insert_into_db = PythonOperator(
            task_id="insert_into_db",
            python_callable=run_csv_to_db_script,
        )

    download_zip_file >> process_and_save_csv >> insert_into_db