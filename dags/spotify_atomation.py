import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka3 import KafkaProducer, KafkaConsumer
from kafka3.errors import kafka_errors
from airflow.utils.task_group import TaskGroup
import requests
import logging
import psycopg2
from airflow.models import Variable
import sys
from utils.DiscordNotifier import DiscordNotifier
from refresh_token import get_latest_ac_token, request_new_ac_token_refresh_token
import time

# Variable.set("db_host", "sp_project-postgres-1")
Variable.set("db_hostname", "postgres")
Variable.set("db_port", "5432")
Variable.set("db_name", "spotify")
Variable.set("db_user", "airflow")
Variable.set("db_password", "airflow")
Variable.set("broker", "broker:29092")
Variable.set("group_id", "test-consumer-group")

## Getting Enviroment Variable ##
db_host = Variable.get("db_host")
db_hostname = Variable.get("db_hostname")
db_port = Variable.get("db_port")
db_name = Variable.get("db_name")
db_user = Variable.get("db_user")
db_password = Variable.get("db_password")
broker = Variable.get("broker")
group_id = Variable.get("group_id")


## you may change the email to yours, if you want to change the sender's info, you may go config/airflow.cfg replace [smpt] related information.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    # 'email': ['a1752815@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_success': True
    'on_failure_callback': DiscordNotifier(msg=" ⚠️️Task Run Failed!⚠️"),
    'on_success_callback': DiscordNotifier(msg=" ✅️Task Run Success!✅")
}

def check_if_need_update_token(**context):
    current_timestamp = int(time.time())
    last_ac_time = get_latest_ac_token()[1]
    access_token = get_latest_ac_token()[0]
    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token()
    context['ti'].xcom_push(key='access_token', value=access_token)

def get_data(**context):

        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token', key='access_token')
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        scope_of_2024 = {"01": 31, "02": 29, "03": 31}

        for month, last_date_of_month in scope_of_2024.items():
            year = "2024"
            days, end = 1, last_date_of_month

            while days <= end:
                headers = {
                    'authority': 'charts-spotify-com-service.spotify.com',
                    'accept': 'application/json',
                    'accept-language': 'zh-TW,zh;q=0.9',
                    'app-platform': 'Browser',
                    'authorization': f'Bearer {access_token}',
                    'content-type': 'application/json',
                    'origin': 'https://charts.spotify.com',
                    'referer': 'https://charts.spotify.com/',
                    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Brave";v="122"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'sec-gpc': '1',
                    'spotify-app-version': '0.0.0.production',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                }

                logging.info(f"Using access_token:{access_token}")
                url = f'https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-global-daily/{year}-{month}-{str(days).zfill(2)}'
                logging.info(f"Using url: {url}")
                response = requests.get(
                    url,
                    headers=headers,
                )

                entries = response.json()["entries"]

                producer.send(f"2024{month}", json.dumps(entries).encode('utf-8'))
                logging.info(f'data sent: {entries}')
                days += 1

                n = random.randint(1,3) ## gen 1~3s
                time.sleep(n)




with DAG('spotify_atomation.py',
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    check_if_need_update_token = PythonOperator(
        task_id='check_if_need_update_token',
        python_callable=check_if_need_update_token
    )
    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        provide_context=True,
    )

check_if_need_update_token>> get_data