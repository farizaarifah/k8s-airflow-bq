from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import psycopg2
import pandas as pd
from datetime import datetime
import os
import yaml

from airflow.configuration import conf
DAGS_FOLDER = conf.get("core", "dags_folder")

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
def fetch_and_ingest(table_name, yaml_file, **context):
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
    query = "SELECT * FROM colms.{}".format(table_name)
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

    #take schema
    with open(yaml_file, 'r') as file:
        schema_yaml = yaml.safe_load(file)
    schemas = [bigquery.SchemaField(col['name'], col['type']) for col in schema_yaml['schema']]


    # Ingest data into BigQuery
    client = bigquery.Client('alami-group-data')

    # Define your dataset and table name
    dataset_id = 'temp_7_days'
    table_id = f'colms_{table_name}'
    table_id= dataset_id + "." + table_id

    # dari dokumentasi
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        # schema=[
        #     # Specify the type of columns whose type cannot be auto-detected. For
        #     # example the "title" column uses pandas dtype "object", so its
        #     # data type is ambiguous.
        #     bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
        #     # Indexes are written if included in the schema by name.
        #     bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
        # ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        schema=schemas,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
tables = [
    {'name': 'company', 'schema': './schema_tables/company.yaml'}
]

for table in tables:
# Create a PythonOperator to run the fetch and ingest function
    # fetch_and_ingest_task = PythonOperator(
    #     task_id='fetch_and_ingest_task',
    #     python_callable=fetch_and_ingest,
    #     dag=dag,
    # )
    fetch_and_ingest_task = PythonOperator(
            task_id=f'load_{table['name']}_to_bq',
            python_callable=fetch_and_ingest,
            provide_context=True,
            op_args={'table_name': table['name'], 'yaml_file': table['schema']},
            dag=dag,
        )

# Set task dependencies if needed (for this example, there's only one task)
fetch_and_ingest_task