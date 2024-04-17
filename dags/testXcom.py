from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_function(**context):
    value = "Hello, world!"
    context['ti'].xcom_push(key='my_key', value=value)

def pull_function(**context):
    ti = context['ti']
    pulled_value = ti.xcom_pull(task_ids='push_task', key='my_key')
    print("Pulled value:", pulled_value)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
}

with DAG('xcom_example', default_args=default_args, schedule_interval=None) as dag:
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_function,
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
    )

    push_task >> pull_task
