import json
import requests
import base64
import psycopg2
import time

def create_connection():
    try:
        conn = psycopg2.connect(
            dbname='spotify',
            user='airflow',
            password='airflow',
            host='localhost',
            port='5432'
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def get_latest_ac_token(current_worker):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"select * from {current_worker}\n"
        f"order by access_last_update desc limit 1"
    )
    rows = cursor.fetchall()
      # Assuming access_token is the first column and refresh_token is the second column
    return rows[0]


def get_latest_refresh_token(current_worker):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"select * from {current_worker}\n"
        f"order by refresh_last_update desc limit 1"
    )
    rows = cursor.fetchall()
      # Assuming access_token is the first column and refresh_token is the second column
    return rows[0]


def request_new_ac_token_refresh_token(current_worker,client_id,client_secret):

    refresh_token = get_latest_refresh_token(current_worker)[2]
    client_id = client_id
    client_secret = client_secret
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    data = {
        'client_id': client_id,
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
            f"INSERT INTO {current_worker} (access_token, access_last_update, refresh_token, refresh_last_update) \
             VALUES (%s, %s, %s, %s)",
            (access_token, current_timestamp, refresh_token, current_timestamp)
        )
        conn.commit()
        print(f"Token successfully updated: {access_token}")
        return access_token

    except Exception as e:
        print(f"Exception occur: {e}")