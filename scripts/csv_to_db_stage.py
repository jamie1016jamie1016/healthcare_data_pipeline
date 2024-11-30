import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from sqlalchemy.engine.url import make_url
import traceback  # Import traceback for detailed error logging

# Extract DB connection details from the environment variable
db_url = make_url(os.environ.get('AIRFLOW__EXTRA__DB_CONN'))
DB_HOST = db_url.host
DB_PORT = db_url.port
DB_NAME = db_url.database
DB_USER = db_url.username
DB_PASSWORD = db_url.password
CSV_FILE = "dags/processed_outpatient.csv"
TABLE_NAME = "staging_table"

# Step 1: Load the CSV file into a DataFrame
df = pd.read_csv(CSV_FILE, low_memory=False)

# Step 2: Generate the CREATE TABLE statement dynamically
def generate_create_table_query(df, table_name):
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            pg_type = "NUMERIC"
        elif "float" in str(dtype):
            pg_type = "FLOAT"
        elif "datetime" in str(dtype):
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"
        columns.append(f"{col} {pg_type}")
    
    columns_sql = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
    return create_table_query

# Generate the query
create_table_query = generate_create_table_query(df, TABLE_NAME)

# Step 3: Connect to PostgreSQL and create the table
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cursor = conn.cursor()
    
    # Create table
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Table {TABLE_NAME} created successfully.")
    
    # Step 4: Insert data into the table
    # Generate the INSERT INTO statement dynamically
    columns = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    insert_query = sql.SQL(
        f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
    )
    
    # Insert rows in batches
    batch_size = 10000  # Adjust batch size as needed
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        cursor.executemany(insert_query, batch.values.tolist())
        conn.commit()
        print(f"Inserted rows {i} to {i + len(batch)}.")
    
except Exception as e:
    print(f"Error occurred: {e}")
    print("Detailed traceback:")
    print(traceback.format_exc())  # Print the full traceback for debugging
finally:
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'conn' in locals() and conn is not None:
        conn.close()
