from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'catchup': False
}

dag = DAG(
    'First_K8s',
    default_args = default_args,
    schedule = None
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

t1 = BashOperator(
    task_id = 'first_task',
    bash_command ='echo "first task"',
    dag = dag
)


t2 = BashOperator(
    task_id = 'second_task',
    bash_command ='echo "second task"',
    dag = dag
)

start_task >> t1 >> t2 >> end_task