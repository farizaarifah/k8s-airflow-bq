import psycopg2
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account

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

# BigQuery detail
project_id = "alami-group-data"
dataset_table = "temp_7_days.colms_company"
credentials = service_account.Credentials.from_service_account_file('/Users/arifahfariza/Documents/repo_alami_env/application_default_credentials_alami.json')

# Memasukkan data ke BigQuery
to_gbq(df, dataset_table, project_id=project_id, if_exists='replace', credentials=credentials)

print("Data berhasil dimasukkan ke BigQuery")