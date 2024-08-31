from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Definisi fungsi untuk mencetak pesan
def print_message():
    print("Hello, this is a simple print message from your Airflow DAG!")

# Definisi default_args untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisialisasi DAG
dag = DAG(
    'simple_print_dag',
    default_args=default_args,
    description='A simple DAG to print a message',
    schedule_interval=timedelta(days=1),
)

# Definisi tugas untuk mencetak pesan
print_task = PythonOperator(
    task_id='print_task',
    python_callable=print_message,
    dag=dag,
)

# Setel urutan eksekusi tugas
print_task