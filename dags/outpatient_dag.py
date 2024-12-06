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
import os

# Define variables from environment variables or fallback defaults
OUTPUT_DIR = os.getenv("AIRFLOW_OUTPUT_DIR")  # Directory for processed files
DELIMITER = os.getenv("CSV_DELIMITER")  # Delimiter for CSV files

default_args = {
    "owner": "airflow",  # Owner of the DAG
    "retries": 1,  # Number of retries in case of failure
    "retry_delay": timedelta(minutes=5),  # Delay between retries
}

def process_zip_data(ti, output_dir, delimiter):
    """
    Process zip data: extract the first CSV, rename, and save with a specific delimiter.
    Parameters:
        - ti: Task Instance to pull data from XCom.
        - output_dir: Directory to save the processed file.
        - delimiter: Delimiter for the CSV file.
    """
    # Retrieve the base64 encoded ZIP content from XCom
    encoded_content = ti.xcom_pull(task_ids="download_zip_file")
    
    # Decode base64 to binary data
    zip_content = base64.b64decode(encoded_content)
    
    # Wrap the bytes in a BytesIO object for file-like operations
    zip_content_io = io.BytesIO(zip_content)
    with zipfile.ZipFile(zip_content_io, 'r') as zip_file:
        for file_name in zip_file.namelist():
            if file_name.endswith('.csv'):  # Process only CSV files
                with zip_file.open(file_name) as csv_file:
                    # Read the CSV file into a DataFrame
                    df = pd.read_csv(csv_file, sep=delimiter)  # Initial delimiter assumed as ','
                    # Save the file with the specified delimiter
                    output_file_path = os.path.join(output_dir, f"processed_{file_name}")
                    df.to_csv(output_file_path, index=False, sep=',')
                    print(f"Processed file saved to {output_file_path}")

def run_csv_to_db_script():
    """
    Run the CSV-to-DB script to insert processed data into the database.
    """
    try:
        # Run the external script
        result = subprocess.run(
            ["python3", "/opt/airflow/scripts/csv_to_db_stage.py"],  # Path to the script
            check=True,
            capture_output=True,  # Capture stdout and stderr
            text=True,  # Decode output as string
            env=os.environ.copy(),  # Pass environment variables
        )
        print("CSV to DB script output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("CSV to DB script failed:", e.stderr)
        raise RuntimeError(f"CSV to DB script failed with error: {e.stderr}")

def create_http_connection():
    """
    Programmatically create an HTTP connection in Airflow.
    """
    conn_id = "cms_data"
    session = settings.Session()  # Initialize a session
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="http",
            host="https://data.cms.gov",  # Endpoint for the CMS data
        )
        session.add(new_conn)
        session.commit()
        print(f"Connection {conn_id} created.")
    else:
        print(f"Connection {conn_id} already exists.")
    session.close()

# Create the HTTP connection if it doesn't exist
create_http_connection()

# Define the DAG
with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="A data pipeline that downloads a ZIP file, extracts CSV, and saves to files",
    schedule_interval=None,  # No scheduled runs; triggered manually
    start_date=datetime(2024, 11, 11),  # Start date for the DAG
    catchup=False,  # Do not backfill previous runs
) as dag:

    # Task 1: Download ZIP file from the HTTP endpoint
    download_zip_file = HttpOperator(
        task_id="download_zip_file",
        method="GET",  # HTTP GET method
        http_conn_id="cms_data",  # Connection ID created programmatically
        endpoint="/sites/default/files/2023-04/c3d8a962-c6b8-4a59-adb5-f0495cc81fda/Outpatient.zip",
        response_filter=lambda response: base64.b64encode(response.content).decode("utf-8"),
        log_response=True,
        do_xcom_push=True,  # Push response data to XCom for downstream tasks
    )

    # Task 2: Process and save CSV from the downloaded ZIP file
    process_and_save_csv = PythonOperator(
        task_id="process_and_save_csv",
        python_callable=process_zip_data,
        op_kwargs={
            "output_dir": OUTPUT_DIR,
            "delimiter": DELIMITER,
        },
    )

    # Task 3: Run the script to insert CSV data into the database
    insert_into_db = PythonOperator(
        task_id="insert_into_db",
        python_callable=run_csv_to_db_script,
    )

    # Define task dependencies
    download_zip_file >> process_and_save_csv >> insert_into_db
