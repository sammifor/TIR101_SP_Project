import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import json
import requests
import logging
from utils.DiscordNotifier import DiscordNotifier
import time
from refresh_token_gcp_Sammi import create_bigquery_client
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from refresh_token_gcp_Sammi import get_latest_ac_token_gcp, request_new_ac_token_refresh_token_gcp
from refresh_token_gcp_Sammi2 import get_latest_ac_token_gcp1, request_new_ac_token_refresh_token_gcp1
from refresh_token_gcp_Sammi3 import get_latest_ac_token_gcp2, request_new_ac_token_refresh_token_gcp2
from refresh_token_gcp_Sammi1 import get_latest_ac_token_gcp3, request_new_ac_token_refresh_token_gcp3
from google.oauth2 import service_account
import pandas as pd
from google.cloud import storage
from requests.exceptions import ConnectionError, Timeout, RequestException
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
    ac_dict = get_latest_ac_token_gcp()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for retry1
def check_if_need_update_token1(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp1()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp1()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for retry2
def check_if_need_update_token2(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp2()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp2()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for retry3
def check_if_need_update_token3(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp3()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp3()

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
    SELECT DISTINCT trackUri
    FROM `affable-hydra-422306-r3.ChangeDataType.expand_orderbydate_table`
    """
    df = client.query(query).to_dataframe() 

    return df  # return trackUri

# filter TrackUri list
def filter_track_uris(track_uris, last_track_uri):
    # Find the index of last_track_uri in track_uris
    if last_track_uri in track_uris:
        last_index = track_uris.index(last_track_uri)
        # Keep only the elements after the last_track_uri
        track_uris = track_uris[last_index + 1:]
    return track_uris

# make sure the last is the end of track_uris
def is_last_uri_last_in_list(track_uris, last_track_uri):
    # Check if last_track_uri is the last item in track_uris
    return track_uris[-1] == last_track_uri if track_uris else False

# store progress data in gcs
def save_progress_to_gcs(client, progress):
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(f"process/track_audio_features_progress.json")
    blob.upload_from_string(json.dumps(progress), content_type='application/json')

# First try - Get Track's Audio features API always failed
def get_track_audio_features_data(**context):
    df = get_track_uris()  # from BigQuery
    track_uris = list(set(df['trackUri'])) #distinct TrackUri

    # get Spotify token
    access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token', key='access_token')

    trackAudioFeatures_list = []
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
        get_track_url = f"https://api.spotify.com/v1/audio-features/{track_uri}"

        response = requests.get(get_track_url, headers=headers,verify=False)

        while response.status_code == 429:
            logging.info(f"Reach the request limitation, saving the progress now!")
            time.sleep(30)

            client = get_storage_client(CREDENTIAL_PATH)

            progress = {
                "last_track_uri": None,
                "trackAudioFeatures_list": []
                }

            progress["last_track_uri"] = track_uri
            progress["trackAudioFeatures_list"] = trackAudioFeatures_list

            save_progress_to_gcs(client, progress)  # save progress to GCS
            logging.error("Progress saved, exiting now!")
            raise Exception("Exceeded retry limit, exiting.")  # raise error make sure task failed
            
        if response.status_code != 200 and response.status_code != 429:# token expired
            logging.info(f"Request a new token for retry")
            access_token = request_new_ac_token_refresh_token_gcp()
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
        
        track_data = response.json()
        trackAudioFeatures_list.append(track_data)

        n = random.randint(1,15) ## gen 1~15s
        time.sleep(n)

        r_count += 1
        if r_count == 150:
            time.sleep(70)
            logging.info(f'{r_count} : time sleep 70s now!')
            r_count = 0
        
    logging.info("If you see this, means you get the whole data from get_track_audio_features API!")
    save_progress_to_gcs(client, trackAudioFeatures_list)  # save final to GCS

## Read progress from gcs
def get_track_audio_features_data1(**context):
    #change sp token
    access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token1', key='access_token')
    
    # try reload progress from GCS
    client = get_storage_client(CREDENTIAL_PATH)
    progress = {
        "last_track_uri": None,
        "trackAudioFeatures_list": []
    }
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/track_audio_features_progress.json")

    if blob.exists():
        progress = json.loads(blob.download_as_text())

    trackAudioFeatures_list = progress["trackAudioFeatures_list"]
    last_track_uri = progress["last_track_uri"]

    # get track URI from BigQuery
    df = get_track_uris()
    track_uris_bq = list(set(df['trackUri']))  # distinct TrackUri

    # filter track_uris list
    track_uris = filter_track_uris(track_uris_bq, last_track_uri)

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
        get_track_url = f"https://api.spotify.com/v1/audio-features/{track_uri}"

        response = requests.get(get_track_url, headers=headers,verify=False)

        while response.status_code == 429:
            logging.info(f"Reach the request limitation, saving the progress now!")
            time.sleep(30)

            client = get_storage_client(CREDENTIAL_PATH)

            progress = {
                "last_track_uri": None,
                "trackAudioFeatures_list": []
                }

            progress["last_track_uri"] = track_uri
            progress["trackAudioFeatures_list"] = trackAudioFeatures_list

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
        
        track_data = response.json()
        trackAudioFeatures_list.append(track_data)

        n = random.randint(1,5) ## gen 1~5s
        time.sleep(n)

        r_count += 1
        if r_count == 150:
            time.sleep(70)
            logging.info(f'{r_count} : time sleep 70s now!')
            r_count = 0 

    logging.info("If you see this, means you get the whole data from get_track_audio_features API!")
    save_progress_to_gcs(client, trackAudioFeatures_list)  # save final to GCS

## Read progress from gcs 1
def get_track_audio_features_data2(**context):

    # try reload progress from GCS
    client = get_storage_client(CREDENTIAL_PATH)
    progress = {
        "last_track_uri": None,
        "trackAudioFeatures_list": []
    }
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/track_audio_features_progress.json")

    if blob.exists():
        progress = json.loads(blob.download_as_text())

    trackAudioFeatures_list = progress["trackAudioFeatures_list"]
    last_track_uri = progress["last_track_uri"]

    # get track URI from BigQuery
    df = get_track_uris()
    track_uris_bq = list(set(df['trackUri']))  # distinct TrackUri

    if is_last_uri_last_in_list(track_uris_bq, last_track_uri):
        logging.info("It is the last item in the list")

    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token2', key='access_token')

        # filter track_uris list
        track_uris = filter_track_uris(track_uris_bq, last_track_uri)

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
            get_track_url = f"https://api.spotify.com/v1/audio-features/{track_uri}"

            response = requests.get(get_track_url, headers=headers,verify=False)

            while response.status_code == 429:
                logging.info(f"Reach the request limitation, saving the progress now!")
                time.sleep(30)

                client = get_storage_client(CREDENTIAL_PATH)

                progress = {
                    "last_track_uri": None,
                    "trackAudioFeatures_list": []
                    }

                progress["last_track_uri"] = track_uri
                progress["trackAudioFeatures_list"] = trackAudioFeatures_list

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
            
            track_data = response.json()
            trackAudioFeatures_list.append(track_data)

            n = random.randint(1,5) ## gen 1~5s
            time.sleep(n)

            r_count += 1
            if r_count == 150:
                time.sleep(70)
                logging.info(f'{r_count} : time sleep 70s now!')
                r_count = 0 

        logging.info("If you see this, means you get the whole data from get_track_audio_features API!")
        save_progress_to_gcs(client, trackAudioFeatures_list)  # save final to GCS

## Read progress from gcs 2
def get_track_audio_features_data3(**context):
# try reload progress from GCS
    client = get_storage_client(CREDENTIAL_PATH)
    progress = {
        "last_track_uri": None,
        "trackAudioFeatures_list": []
    }
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/track_audio_features_progress.json")

    if blob.exists():
        progress = json.loads(blob.download_as_text())

    trackAudioFeatures_list = progress["trackAudioFeatures_list"]
    last_track_uri = progress["last_track_uri"]

    # get track URI from BigQuery
    df = get_track_uris()
    track_uris_bq = list(set(df['trackUri']))  # distinct TrackUri

    if is_last_uri_last_in_list(track_uris_bq, last_track_uri):
        logging.info("It is the last item in the list")

    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token3', key='access_token')
        
        # filter track_uris list
        track_uris = filter_track_uris(track_uris_bq, last_track_uri)

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
            get_track_url = f"https://api.spotify.com/v1/audio-features/{track_uri}"

            response = requests.get(get_track_url, headers=headers,verify=False)

            while response.status_code == 429:
                logging.info(f"Reach the request limitation, saving the progress now!")
                time.sleep(30)

                client = get_storage_client(CREDENTIAL_PATH)

                progress = {
                    "last_track_uri": None,
                    "trackAudioFeatures_list": []
                    }

                progress["last_track_uri"] = track_uri
                progress["trackAudioFeatures_list"] = trackAudioFeatures_list

                save_progress_to_gcs(client, progress)  # save progress to GCS
                logging.error("Progress saved, exiting now!")
                raise Exception("Exceeded retry limit, exiting.")  # raise error make sure task failed
                
            if response.status_code != 200 and response.status_code != 429:# token expired
                logging.info(f"Request a new token for retry")
                access_token = request_new_ac_token_refresh_token_gcp3()
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
            
            track_data = response.json()
            trackAudioFeatures_list.append(track_data)

            n = random.randint(1,5) ## gen 1~5s
            time.sleep(n)

            r_count += 1
            if r_count == 150:
                time.sleep(70)
                logging.info(f'{r_count} : time sleep 70s now!')
                r_count = 0 

        logging.info("If you see this, means you get the whole data from get_track_audio_features API!")
        save_progress_to_gcs(client, trackAudioFeatures_list)  # save final to GCS



def process_data_from_gcs():
    
    client = get_storage_client(CREDENTIAL_PATH)
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/track_audio_features_progress.json")

    trackAudioFeatures_list = json.loads(blob.download_as_text())
    
    #Extend data
    df_trackAudioFeatures = pd.json_normalize(trackAudioFeatures_list)

    #Upload to GCS
    local_file_path = 'AudioFeatures_copy.csv'
    gcs_bucket = 'api_spotify_artists_tracks'
    gcs_file_name = f'output/{local_file_path}'

    df_trackAudioFeatures.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)



with DAG('spotify_GetTrackAudioFeatures_Sammi_copy.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    check_if_need_update_token = PythonOperator(
        task_id='check_if_need_update_token',
        python_callable=check_if_need_update_token
    )

    check_if_need_update_token1 = PythonOperator(
        task_id='check_if_need_update_token1',
        python_callable=check_if_need_update_token1
    )

    check_if_need_update_token2 = PythonOperator(
        task_id='check_if_need_update_token2',
        python_callable=check_if_need_update_token2
    )

    check_if_need_update_token3 = PythonOperator(
        task_id='check_if_need_update_token3',
        python_callable=check_if_need_update_token3
    )

    get_track_audio_features_data = PythonOperator(
        task_id='get_track_audio_features_data',
        python_callable=get_track_audio_features_data,
        provide_context=True,
    )

    get_track_audio_features_data1 = PythonOperator(
        task_id='get_track_audio_features_data1',
        python_callable=get_track_audio_features_data1,
        provide_context=True,
    )

    get_track_audio_features_data2 = PythonOperator(
        task_id='get_track_audio_features_data2',
        python_callable=get_track_audio_features_data2,
        provide_context=True,
    )

    get_track_audio_features_data3 = PythonOperator(
        task_id='get_track_audio_features_data3',
        python_callable=get_track_audio_features_data3,
        provide_context=True,
    )

    process_data_from_gcs = PythonOperator(
        task_id='process_data_in_gcs',
        python_callable=process_data_from_gcs,
        provide_context=True,
    )

# Order of DAGs
check_if_need_update_token >> get_track_audio_features_data >> check_if_need_update_token1 >> get_track_audio_features_data1 >> check_if_need_update_token2 >> get_track_audio_features_data2 >> check_if_need_update_token3 >> get_track_audio_features_data3 >> process_data_from_gcs

#執行data1之前要先執行update token
check_if_need_update_token1.set_upstream(get_track_audio_features_data)
get_track_audio_features_data1.set_upstream(check_if_need_update_token1)
check_if_need_update_token1.trigger_rule = TriggerRule.ALL_FAILED # get_track_audio_features_data失敗時執行

# 執行data2之前要先執行data1
check_if_need_update_token2.set_upstream(get_track_audio_features_data1)
get_track_audio_features_data2.set_upstream(check_if_need_update_token2)
check_if_need_update_token2.trigger_rule = TriggerRule.ALL_DONE
get_track_audio_features_data2.trigger_rule = TriggerRule.ALL_DONE

# # 執行data3之前要先執行data2
check_if_need_update_token3.set_upstream(get_track_audio_features_data2)
get_track_audio_features_data3.set_upstream(check_if_need_update_token3)
check_if_need_update_token3.trigger_rule = TriggerRule.ALL_DONE
get_track_audio_features_data3.trigger_rule = TriggerRule.ALL_DONE

#data1&data2兩個上游任務執行完成才會執行process_data_from_gcs
process_data_from_gcs.set_upstream(
    [get_track_audio_features_data1, get_track_audio_features_data2, get_track_audio_features_data3]
)
process_data_from_gcs.trigger_rule = TriggerRule.ALL_DONE
