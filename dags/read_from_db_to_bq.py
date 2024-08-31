from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import psycopg2
import pandas as pd
from datetime import datetime
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 30),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'postgresql_to_bigquery',
    default_args=default_args,
    schedule_interval=None,  # Set your schedule
)

# Define the function to fetch data from PostgreSQL
def fetch_and_ingest():
    conn = psycopg2.connect(
        # DB_COLMS_NAME_STG
        # DB_COLMS_PASSWORD_STG
        # DB_COLMS_PORT_STG
        # DB_COLMS_SERVER_STG
        # DB_COLMS_USERNAME_STG
        host=os.environ.get("DB_COLMS_SERVER_STG"),
        user=os.environ.get("DB_COLMS_USERNAME_STG"),
        password=os.environ.get("DB_COLMS_PASSWORD_STG"),
        database=os.environ.get("DB_COLMS_NAME_STG"),
        port=os.environ.get("DB_COLMS_PORT_STG"),
        connect_timeout=5
    )
    cursor = conn.cursor()

    # Define your SQL query to fetch data
    query = "SELECT * FROM colms.company"
    cursor.execute(query)

    # Fetch all rows from the query result
    data = cursor.fetchall()
    # Ambil nama kolom dari tabel
    columns = [desc[0] for desc in cursor.description]

    # Konversi data ke DataFrame pandas
    df = pd.DataFrame(data, columns=columns)
    df = df.astype(str)
    print(df)

    # Close the connection
    cursor.close()
    conn.close()

    # return df

    # Ingest data into BigQuery
    client = bigquery.Client('alami-group-data')
    
    # Define your dataset and table name
    dataset_id = 'temp_7_days'
    table_id = 'colms_company'
    
    # Convert DataFrame to a list of dictionaries for BigQuery
    rows_to_insert = df.to_dict(orient='records')
    
    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    # Insert the data into BigQuery
    errors = client.insert_rows_json(table, rows_to_insert)
    
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print("Data ingested successfully.")

# Create a PythonOperator to run the fetch and ingest function
fetch_and_ingest_task = PythonOperator(
    task_id='fetch_and_ingest_task',
    python_callable=fetch_and_ingest,
    dag=dag,
)

# Set task dependencies if needed (for this example, there's only one task)
fetch_and_ingest_task