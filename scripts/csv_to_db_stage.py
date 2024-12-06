import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from sqlalchemy.engine.url import make_url
import traceback  # Import traceback for detailed error logging

# Extract database connection details from environment variables
db_url = make_url(os.environ.get('AIRFLOW__EXTRA__DB_CONN'))  # Connection string from environment
DB_HOST = db_url.host
DB_PORT = db_url.port
DB_NAME = db_url.database
DB_USER = db_url.username
DB_PASSWORD = db_url.password

# Define the path to the CSV file (can be customized via environment variable)
CSV_FILE = os.getenv("CSV_FILE_PATH")

# Define the table name for staging the data
TABLE_NAME = os.getenv("TABLE_NAME")

# Step 1: Load the CSV file into a pandas DataFrame
try:
    print(f"Loading data from CSV file: {CSV_FILE}")
    df = pd.read_csv(CSV_FILE, low_memory=False)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns from the CSV file.")
except Exception as e:
    print(f"Error loading CSV file: {e}")
    raise

# Step 2: Generate the CREATE TABLE statement dynamically
def generate_create_table_query(df, table_name):
    """
    Generates a SQL CREATE TABLE query based on the DataFrame schema.
    Parameters:
        - df: pandas DataFrame containing the data.
        - table_name: Name of the table to be created.
    Returns:
        - SQL query string to create the table.
    """
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        # Map pandas data types to PostgreSQL data types
        if "int" in str(dtype):
            pg_type = "NUMERIC"
        elif "float" in str(dtype):
            pg_type = "FLOAT"
        elif "datetime" in str(dtype):
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"  # Default to TEXT for other types
        columns.append(f"{col} {pg_type}")
    
    # Combine all column definitions into a CREATE TABLE query
    columns_sql = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
    return create_table_query

# Generate the CREATE TABLE query
create_table_query = generate_create_table_query(df, TABLE_NAME)

# Step 3: Connect to PostgreSQL and create the table
try:
    print("Connecting to the PostgreSQL database...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cursor = conn.cursor()

    # Execute the CREATE TABLE query
    print(f"Creating table: {TABLE_NAME}")
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Table {TABLE_NAME} created successfully.")

    # Step 4: Insert data into the table
    print("Preparing to insert data into the table...")
    # Generate the INSERT INTO statement dynamically
    columns = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    insert_query = sql.SQL(
        f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
    )
    
    # Insert rows in batches
    batch_size = int(os.getenv("BATCH_SIZE", 10000))  # Batch size can be adjusted via environment variable
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        cursor.executemany(insert_query, batch.values.tolist())
        conn.commit()
        print(f"Inserted rows {i + 1} to {i + len(batch)}.")
    
    print("Data insertion completed successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
    print("Detailed traceback:")
    print(traceback.format_exc())  # Print full traceback for debugging
finally:
    # Ensure resources are closed properly
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'conn' in locals() and conn is not None:
        conn.close()
    print("Database connection closed.")
