import psycopg2
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

# Informasi koneksi ke database
conn_params = {
    "host": "172.20.13.33",
    "dbname": "colms_stg",
    "user": "dataengineer",
    "password": "Kjfnh34Hd48Ihd",
    "port": "5432"
}

# Membuat koneksi ke PostgreSQL
# conn = psycopg2.connect(**conn_params)
conn = psycopg2.connect(
    host="172.20.13.33",
    user="dataengineer",
    password="Kjfnh34Hd48Ihd",
    database="colms_stg",
    port="5432",
    connect_timeout=5
)

# Membuat cursor
cursor = conn.cursor()

# Query SQL untuk membaca data
query = "SELECT * FROM colms.company"

# Eksekusi query
cursor.execute(query)

# Ambil semua hasil
data = cursor.fetchall()

# Ambil nama kolom dari tabel
columns = [desc[0] for desc in cursor.description]

# Konversi data ke DataFrame pandas
df = pd.DataFrame(data, columns=columns)
df = df.astype(str)
print(df)

# close cursor dan koneksi
cursor.close()
conn.close()

# Ingest data into BigQuery
client = bigquery.Client('alami-group-data')

# Define your dataset and table name
dataset_id = 'temp_7_days'
table_id = 'colms_company'
table_id= dataset_id + "." + table_id

# # Convert DataFrame to a list of dictionaries for BigQuery
# rows_to_insert = df.to_dict(orient='records')

# # Define the table reference
# table_ref = client.dataset(dataset_id).table(table_id)
# table = client.get_table(table_ref)

# # Insert the data into BigQuery
# errors = client.insert_rows_json(table, rows_to_insert)

# if errors:
#     print(f"Encountered errors while inserting rows: {errors}")
# else:
#     print("Data ingested successfully.")

# # BigQuery detail
# project_id = "alami-group-data"
# dataset_table = "temp_7_days.colms_company"
# credentials = service_account.Credentials.from_service_account_file('/Users/arifahfariza/Documents/repo_alami_env/application_default_credentials_alami.json')

# # Memasukkan data ke BigQuery
# to_gbq(df, dataset_table, project_id=project_id, if_exists='replace', credentials=credentials)

# print("Data berhasil dimasukkan ke BigQuery")

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
