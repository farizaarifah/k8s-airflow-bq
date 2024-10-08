from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import psycopg2
import pandas as pd
from datetime import datetime
import os
import yaml
import pathlib
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
def fetch_and_ingest(table_name, yaml_file, **kwargs):
    conn = psycopg2.connect(
        host=os.environ.get("DB_COLMS_SERVER_STG"),
        user=os.environ.get("DB_COLMS_USERNAME_STG"),
        password=os.environ.get("DB_COLMS_PASSWORD_STG"),
        database=os.environ.get("DB_COLMS_NAME_STG"),
        port=os.environ.get("DB_COLMS_PORT_STG"),
        connect_timeout=5
    )
    cursor = conn.cursor()
    print(table_name, "----------------")
    
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
    # with open(yaml_file, 'r') as file:
    #     schema_yaml = yaml.safe_load(file)
    # schemas = [bigquery.SchemaField(col['name'], col['type']) for col in schema_yaml['schema']]
    # print(schema_yaml['table_name'], "table name dari yaml")


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
        #     bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
        #     # Indexes are written if included in the schema by name.
        #     bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
        # ],
        # schema=schemas,
        schema=yaml_file,
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
    {'name': 'company', 'schema': '/opt/airflow/dags/repo/dags/schema_tables/company.yaml'}
]

current_path = pathlib.Path("/opt/airflow/dags/repo/dags/").absolute()
config_dir_path = current_path.joinpath("schema_tables")

for yaml_path in config_dir_path.glob("*.y*ml"):
    # yml_conf = YAML().load(yaml_path.open("r"))
    with open(yaml_path, 'r') as file:
        yaml_config = yaml.safe_load(file)
    fetch_and_ingest_task = PythonOperator(
        task_id=f'load_{yaml_config['table_name']}_to_bq',
        python_callable=fetch_and_ingest,
        provide_context=True,
        op_args=[yaml_config['table_name'], yaml_config['schema']],
        dag=dag,
    )

# for table in tables:
# Create a PythonOperator to run the fetch and ingest function
    # fetch_and_ingest_task = PythonOperator(
    #         task_id=f'load_{table['name']}_to_bq',
    #         python_callable=fetch_and_ingest,
    #         provide_context=True,
    #         op_args=[table['name'], table['schema']],
    #         dag=dag,
    #     )

# Set task dependencies if needed (for this example, there's only one task)
# fetch_and_ingest_task