import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import requests
import base64
from airflow.models import Variable
import psycopg2
import time

def create_connection():
    try:
        conn = psycopg2.connect(
            dbname='spotify',
            user='airflow',
            password='airflow',
            host='sp_project-postgres-1',
            port='5432'
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def get_latest_ac_token():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(
        "select * from tokens\n"
        "order by access_last_update desc limit 1"
    )
    rows = cursor.fetchall()
      # Assuming access_token is the first column and refresh_token is the second column
    return rows[0]


def get_latest_refresh_token():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(
        "select * from tokens\n"
        "order by refresh_last_update desc limit 1"
    )
    rows = cursor.fetchall()
      # Assuming access_token is the first column and refresh_token is the second column
    return rows[0]


def request_new_ac_token_refresh_token():

    refresh_token = get_latest_refresh_token()[2]
    client_id = '3140d7f560664be9a52544791c13b670'
    client_secret = 'cfb94502d418483a9fa631e4d7d23691'
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    data = {
        'client_id': '3140d7f560664be9a52544791c13b670',
        'grant_type': 'refresh_token',
        'refresh_token': f'{refresh_token}'
    }
    headers = {'content-type': 'application/x-www-form-urlencoded',
               'Authorization': f"Basic {encoded_credentials}",
               }
    response = requests.post('https://accounts.spotify.com/api/token', data=data, headers=headers,timeout=4)
    access_token = response.json()['access_token']
    if 'refresh_token' not in response.json():
        refresh_token = refresh_token
    else:
        refresh_token = response.json()['refresh_token']


    conn = create_connection()
    cursor = conn.cursor()
    current_timestamp = int(time.time())
    try:
        cursor.execute(
            "INSERT INTO tokens (access_token, access_last_update, refresh_token, refresh_last_update) \
             VALUES (%s, %s, %s, %s)",
            (access_token, current_timestamp, refresh_token, current_timestamp)
        )
        conn.commit()
        print(f"Token successfully updated: {access_token}")
        return access_token

    except Exception as e:
        print(f"Exception occur: {e}")