import requests
import base64
import time
from utils.GCP_client import get_bq_client
import os 
import logging

CREDENTIAL_PATH = os.environ.get('CREDENTIAL_PATH')

def get_latest_ac_token(current_worker):
    client = get_bq_client(CREDENTIAL_PATH)
    with client:
        query = f"""
            SELECT * 
            FROM affable-hydra-422306-r3.worker.{current_worker}
            ORDER BY access_last_update DESC LIMIT 1
        """
        query_job = client.query(query)
        row = query_job.result()
        logging.info("Fetching latest refresh token from BigQuery...")
        # access_token, access_last_update, refresh_token, refresh_last_update
    return row

# get current workers' refresh token from GCP
def get_latest_refresh_token(current_worker):
    client = get_bq_client(CREDENTIAL_PATH)
    with client:
        query = f"""
        SELECT * 
        FROM affable-hydra-422306-r3.worker.{current_worker}
        ORDER BY refresh_last_update DESC LIMIT 1
        """
        query_job = client.query(query)
        row = query_job.result()
        logging.info("Fetching latest refresh token from BigQuery...")
        # access_token, access_last_update, refresh_token, refresh_last_update
    return row


def request_new_ac_token_refresh_token(current_worker,client_id,client_secret):

    refresh_token = get_latest_refresh_token(current_worker)
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


    client = get_bq_client(CREDENTIAL_PATH)
    current_timestamp = int(time.time())
    try:
        query = f"""
            INSERT INTO {current_worker} (access_token, access_last_update, refresh_token, refresh_last_update)
            VALUES (%s, %s, %s, %s)",
            (access_token, {current_timestamp}, refresh_token, {current_timestamp})
        """
        client.query(query)
        print(f"Token successfully updated: {access_token}")
        return access_token

    except Exception as e:
        print(f"Exception occur: {e}")