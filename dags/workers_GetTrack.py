import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule 
from worker_refresh_token import get_latest_token, request_new_ac_token_refresh_token
import itertools
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from pandas import json_normalize
import requests
import json
import time
import random
from datetime import datetime
import itertools
import urllib3
from requests.exceptions import SSLError, ConnectionError
urllib3.disable_warnings()
from utils.workers_clientID import get_workers
from utils.GCP_client import get_bq_client, get_storage_client, save_progress_to_gcs
from utils.trackUri import get_track_uris, filter_track_uris, is_last_uri_last_in_list
from utils.DiscordNotifier import DiscordNotifier
import logging

CREDENTIAL_PATH = os.environ.get('CREDENTIAL_PATH')
BUCKET_FILE_PATH = "process/worker_get_track_progress_1724.json"

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

def check_if_need_update_token(current_worker_name, current_worker):
    current_time = int(time.time())
    logging.info(f"Current worker: {current_worker_name}")

    client_id = current_worker["client_id"]
    secret = current_worker["client_secret"]
    access_last_update = get_latest_token(current_worker_name)[1]
    access_token = get_latest_token(current_worker_name)[0]

    logging.info(f"using access_token: {access_token}")

    if access_last_update + 3500 <= current_time: # means token has been expired, default is 3600, make it 3500 to ensure we have enough time
        # logging.info(f"access_last_update:{access_last_update} +3500 = {access_last_update+3500} current_time:{current_time}")
        access_token = request_new_ac_token_refresh_token(current_worker_name,
        current_worker["client_id"], current_worker["client_secret"])
    
    return access_token

def get_track_data():

    df = get_track_uris(CREDENTIAL_PATH)
    track_uris = list(set(df['trackUri'])) #distinct TrackUri

    trackAudioFeatures_list = []
    start_time = int(time.time())

    workers = get_workers()
    #看不懂
    worker_cycle = itertools.cycle(workers.items())
    current_worker_name, current_worker = next(worker_cycle)
    
    #是下面的for迴圈 count % 100
    count = 1

    for track_uri in track_uris:

        current_time = int(time.time())
        elapsed_time = current_time - start_time
        
        if elapsed_time >= 30:
            start_time = current_time
            print(f"{elapsed_time} - Doing switch worker !!")
            current_worker_name, current_worker = next(worker_cycle)
            time.sleep(1)

        if not is_last_uri_last_in_list(track_uris, track_uris):
            access_token = check_if_need_update_token(current_worker_name, current_worker)

            trackData_list = []
            headers = {
                'accept': '*/*',
                'accept-language': 'zh-TW,zh;q=0.8',
                'authorization': f'Bearer {access_token}',
                'origin': 'https://developer.spotify.com',
                'referer': 'https://developer.spotify.com/',
                'sec-ch-ua': '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'sec-gpc': '1',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                'Connection': 'close',
            }
            try:
                get_track_url = f"https://api.spotify.com/v1/tracks/{track_uri}"
                response = requests.get(get_track_url, headers=headers, verify=False)
                
                if response.status_code == 429:
                    logging.info(f"Reach the request limitation, change the worker now!")
                    time.sleep(10)
                    access_token = check_if_need_update_token(current_worker_name, current_worker)
                    response = requests.get(get_track_url,
                                            headers={
                                                'accept': '*/*',
                                                'accept-language': 'zh-TW,zh;q=0.8',
                                                'authorization': f'Bearer {access_token}',
                                                'origin': 'https://developer.spotify.com',
                                                'referer': 'https://developer.spotify.com/',
                                                'sec-ch-ua': '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                                                'sec-ch-ua-mobile': '?0',
                                                'sec-ch-ua-platform': '"macOS"',
                                                'sec-fetch-dest': 'empty',
                                                'sec-fetch-mode': 'cors',
                                                'sec-fetch-site': 'same-site',
                                                'sec-gpc': '1',
                                                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                                                'Connection': 'close',
                                            },
                                            verify=False)

                track_data = response.json()
                trackData_list.append(track_data)

                count += 1
                logging.info(f"{count}-{track_uri}")

                # n = random.randint(1,3)  ## gen 1~3s
                time.sleep(1)

                #每100筆睡2秒
                if count % 100 == 0:
                    time.sleep(2)
                    client = get_storage_client(CREDENTIAL_PATH)

                    progress = {
                        "last_track_uri": track_uri,
                        "trackData_list": trackData_list
                        }

                    save_progress_to_gcs(client, progress, BUCKET_FILE_PATH)  # save progress to GCS
                    
            except (SSLError, ConnectionError) as e:
                response = requests.get(get_track_url, headers=headers, verify=False)
                logging.info(f"get the {e} data again done!")

                client = get_storage_client(CREDENTIAL_PATH)

                progress = {
                    "last_track_uri": track_uri,
                    "trackData_list": trackData_list
                    }

                save_progress_to_gcs(client, progress, BUCKET_FILE_PATH)  # save progress to GCS
                
  
    client=get_bq_client(CREDENTIAL_PATH)
    save_progress_to_gcs(client, trackAudioFeatures_list, BUCKET_FILE_PATH)
    logging.info(f"If you see this, means you get the whole data - {len(trackAudioFeatures_list)} from get_track API!")

def process_data_in_gcs():
    
    client = get_storage_client(CREDENTIAL_PATH)
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(BUCKET_FILE_PATH)

    trackData_list = json.loads(blob.download_as_text())
    
    #Extend data
    df = pd.json_normalize(trackData_list).drop(columns=['album.images','available_markets'])
    df_exploded = df.explode('artists')
    df_artists=pd.json_normalize(df_exploded['artists'])

    #rename
    rename_columns = {'href':'artists.href', 'id':'artists.id', 'name':'artists.name', 'type':'artists.type', 'uri':'artists.uri', 'external_urls.spotify':'artists.external_urls.spotify'}
    df_rename = df_artists.rename(columns=rename_columns)
    df_final = df_exploded.drop(columns='artists')

    df_final['artists.href'] = df_rename['artists.href']
    df_final['artists.id'] = df_rename['artists.id']
    df_final['artists.name'] = df_rename['artists.name']
    df_final['artists.type'] = df_rename['artists.type']
    df_final['artists.uri'] = df_rename['artists.uri']
    df_final['artists.external_urls.spotify'] = df_rename['artists.external_urls.spotify']

    #Upload to GCS
    local_file_path = 'worker_get_track_progress_1724.csv'
    gcs_bucket = 'api_spotify_artists_tracks'
    gcs_file_name = f'output/{local_file_path}'

    df_final.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)

with DAG('workers_GetTrack.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    
    get_track_data = PythonOperator(
        task_id='get_track_data',
        python_callable=get_track_data,
        provide_context=True,
    )

    process_data_in_gcs = PythonOperator(
        task_id='process_data_in_gcs',
        python_callable=process_data_in_gcs,
        provide_context=True,
    )

# Order of DAGs
get_track_data >> process_data_in_gcs