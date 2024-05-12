import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import logging
from utils.DiscordNotifier import DiscordNotifier
import time
from google.cloud import bigquery
from refresh_token.refresh_token_gcp_Sammi7 import get_latest_ac_token_gcp1, request_new_ac_token_refresh_token_gcp1
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

# get TrackUri from BigQuery
def get_track_uris():
    path_credential = "./dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"  
    credentials = service_account.Credentials.from_service_account_file(
        path_credential
    )
    client = bigquery.Client(credentials=credentials)

    # 查询 trackUri
    query = """
    SELECT trackUri
    FROM `affable-hydra-422306-r3.0000.error_analysis`
    """
    df = client.query(query).to_dataframe() 

    return df  # return trackUri

def check_if_need_update_token1(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp1()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp1()

    context['ti'].xcom_push(key='access_token', value=access_token)

# store progress data in gcs 
def save_progress_to_gcs(client, progress):
    
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/err_track_audio_analysis_progress.json")
    blob.upload_from_string(json.dumps(progress), content_type='application/json')

## Read progress from gcs track_uris1
def get_track_audio_analysis_data1(**context):
    #change sp token
    access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token1', key='access_token')
    
    df = get_track_uris()
    track_uris= list(set(df['trackUri']))

    trackAudioAnalysis_list = []
    r_count = 0
    for track_uri in track_uris:
        headers = {
        "authority": "api.spotify.com",
        "Accept": "application/json",
        "accept-language": "zh-TW,zh;q=0.9",
        "app-platform": "Browser",
        "Authorization": f"Bearer {access_token}",
        "content-type": "application/json",
        "Origin": "https://developer.spotify.com",
        "Referer": "https://developer.spotify.com/",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Connection": "close"
    }

        logging.info(f"Using access_token:{access_token}")
        get_track_url = f"https://api.spotify.com/v1/audio-analysis/{track_uri}"

        response = requests.get(get_track_url, headers=headers,verify=False)

        while response.status_code == 429:
            logging.info(f"Reach the request limitation, saving the progress now!")
            time.sleep(10)

            client = get_storage_client(CREDENTIAL_PATH)

            progress = {
                "last_track_uri": track_uri,
                "trackAudioAnalysis_list": trackAudioAnalysis_list
                }

            save_progress_to_gcs(client, progress)  # save progress to GCS
            logging.error("Progress saved, exiting now!")
            raise Exception("Exceeded retry limit, exiting.")  # raise error make sure task failed
            
        if response.status_code != 200 and response.status_code != 429:# token expired
            logging.info(f"Request a new token for retry")
            access_token = request_new_ac_token_refresh_token_gcp1()
            response = requests.get(
                get_track_url,
                headers = {
            "authority": "api.spotify.com",
            "Accept": "application/json",
            "accept-language": "zh-TW,zh;q=0.9",
            "app-platform": "Browser",
            "Authorization": f"Bearer {access_token}",
            "content-type": "application/json",
            "Origin": "https://developer.spotify.com",
            "Referer": "https://developer.spotify.com/",
            "Sec-Ch-Ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Connection": "close"
        },
        verify=False
            )
        
        track_data = response.json()['track']
        trackAudioAnalysis_list.append(track_data)

        n = random.randint(1,3) ## gen 1~5s
        time.sleep(n)

        r_count += 1
        if r_count == 150:
            client = get_storage_client(CREDENTIAL_PATH)

            progress = {
                "last_track_uri": track_uri,
                "trackAudioAnalysis_list": trackAudioAnalysis_list
                }

            save_progress_to_gcs(client, progress)  # save progress to GCS
            time.sleep(70)
            logging.info(f'{r_count} save to gcs - time sleep 70s now!')
            r_count = 0 

    logging.info(f"If you see this, means you get {len(trackAudioAnalysis_list)} data from group1!")
    client = get_storage_client(CREDENTIAL_PATH)

    save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS

def process_data_in_gcs():
    
    client = get_storage_client(CREDENTIAL_PATH)
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/err_track_audio_analysis_progress.json")

    trackAudioAnalysis_list = json.loads(blob.download_as_text())
    # trackAudioAnalysis_list = final_from_gcs["trackAudioAnalysis_list"]
    
    #getTrackUri
    df = get_track_uris()  # from BigQuery
    track_uris = list(set(df['trackUri'])) #distinct TrackUri

    #Extend data
    df_trackUri = pd.DataFrame(track_uris,columns=['trackUri'])
    df_trackAudioAnalysis = pd.DataFrame(trackAudioAnalysis_list)
    df_concat = pd.concat([df_trackUri,df_trackAudioAnalysis], axis=1)
    
    #Upload to GCS
    local_file_path = 'err_trackOfAudioAnalysis.csv'
    gcs_bucket = 'api_spotify_artists_tracks'
    gcs_file_name = f'output/{local_file_path}'

    df_concat.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)


with DAG('err_trackOfAudioAnalysis.py',
        default_args=default_args,
        schedule="@monthly",  
        catchup=False) as dag:
    check_if_need_update_token1 = PythonOperator(
        task_id='check_if_need_update_token1',
        python_callable=check_if_need_update_token1
    )

    get_track_audio_analysis_data1 = PythonOperator(
        task_id='get_track_audio_analysis_data1',
        python_callable=get_track_audio_analysis_data1,
        provide_context=True,
    )

    process_data_in_gcs = PythonOperator(
        task_id='process_data_in_gcs',
        python_callable=process_data_in_gcs,
        provide_context=True,
    )


# Order of DAGs
check_if_need_update_token1 >> get_track_audio_analysis_data1 >> process_data_in_gcs