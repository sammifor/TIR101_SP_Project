import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import logging
from utils.DiscordNotifier import DiscordNotifier
import time
from google.cloud import bigquery
from refresh_token.refresh_token_gcp_Sammi4 import get_latest_ac_token_gcp4, request_new_ac_token_refresh_token_gcp4
from google.oauth2 import service_account
import pandas as pd
from google.cloud import storage
import urllib3
urllib3.disable_warnings()

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

CREDENTIAL_PATH = "./dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"
def get_storage_client(CREDENTIAL_PATH):
    # Load the service account credentials from the specified file
    credentials = service_account.Credentials.from_service_account_file(CREDENTIAL_PATH)
  
    # Create and return a Cloud Storage client using the credentials
    return storage.Client(credentials=credentials)

def check_if_need_update_token(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp4()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp4()

    context['ti'].xcom_push(key='access_token', value=access_token)


# get TrackUri from BigQuery
def get_track_uris():  
    path_credential = "./dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"  
    credentials = service_account.Credentials.from_service_account_file(
        path_credential
    )
    client = bigquery.Client(credentials=credentials)

    # 查询 trackUri
    query = """
    SELECT c_trackUri
    FROM `affable-hydra-422306-r3.0000.err_track`
    """
    df = client.query(query).to_dataframe() 

    return df  # return trackUri

# store progress data in gcs
def save_progress_to_gcs(client, progress):   
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(f"process/err_get_track_progress.json")
    blob.upload_from_string(json.dumps(progress), content_type='application/json')

# First try - Get_Track_API
def get_track_data(**context):
    df = get_track_uris()  # from BigQuery
    track_uris = list(set(df['c_trackUri'])) #distinct TrackUri

    # get Spotify token
    access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token', key='access_token')

    trackData_list = []
    r_count = 0
    for track_uri in track_uris:
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
        "Connection": "close"    
    }

        logging.info(f"Using access_token:{access_token}")
        get_track_url = f"https://api.spotify.com/v1/tracks/{track_uri}"

        response = requests.get(get_track_url, headers=headers,verify=False)

        while response.status_code == 429:
            logging.info(f"Reach the request limitation, saving the progress now!")
            time.sleep(10)

            client = get_storage_client(CREDENTIAL_PATH)

            progress = {
                "last_track_uri": None,
                "trackData_list": []
                }
            
            progress["last_track_uri"] = track_uri
            progress["trackData_list"] = trackData_list

            save_progress_to_gcs(client, progress)  # save progress to GCS
            logging.error("Progress saved, exiting now!")
            raise Exception("Exceeded retry limit, exiting.")  # raise error make sure task failed
        
            
        if response.status_code != 200 and response.status_code != 429:# token expired
            logging.info(f"Request a new token for retry")
            access_token = request_new_ac_token_refresh_token_gcp4()
            response = requests.get(
                get_track_url,
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
                    "Connection": "close"
                },
                verify=False
            )

        track_data = response.json()
        trackData_list.append(track_data)

        logging.info(f'{r_count} - get trackData_list success!')

        r_count += 1
        if r_count == 150:
            time.sleep(70)
            logging.info(f'{r_count} : time sleep 70s now!')
            r_count = 0
    
    logging.info("If you see this, means you get the whole data from get_track API!")
    client = get_storage_client(CREDENTIAL_PATH)
    save_progress_to_gcs(client, trackData_list)  # save progress to GCS

def process_data_in_gcs():
    
    client = get_storage_client(CREDENTIAL_PATH)
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/err_get_track_progress.json")

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
    local_file_path = 'err_getTrack.csv'
    gcs_bucket = 'api_spotify_artists_tracks'
    gcs_file_name = f'output/{local_file_path}'

    df_final.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)


with DAG('err_GetTrack.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    check_if_need_update_token = PythonOperator(
        task_id='check_if_need_update_token',
        python_callable=check_if_need_update_token
    )

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
check_if_need_update_token >> get_track_data >> process_data_in_gcs

