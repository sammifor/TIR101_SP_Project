import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import json
import requests
import logging
from utils.DiscordNotifier import DiscordNotifier
import time
from refresh_token.refresh_token_gcp_Sammi6 import create_bigquery_client
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from refresh_token.refresh_token_gcp_Sammi6 import get_latest_ac_token_gcp, request_new_ac_token_refresh_token_gcp
from refresh_token.refresh_token_gcp_Sammi7 import get_latest_ac_token_gcp1, request_new_ac_token_refresh_token_gcp1
from refresh_token.refresh_token_gcp_Sammi8 import get_latest_ac_token_gcp2, request_new_ac_token_refresh_token_gcp2
from refresh_token.refresh_token_gcp_Sammi9 import get_latest_ac_token_gcp3, request_new_ac_token_refresh_token_gcp3
from refresh_token.refresh_token_gcp_Sammi4 import get_latest_ac_token_gcp4, request_new_ac_token_refresh_token_gcp4
from refresh_token.refresh_token_gcp_Sammi5 import get_latest_ac_token_gcp5, request_new_ac_token_refresh_token_gcp5
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
    FROM `affable-hydra-422306-r3.ChangeDataType.expand_orderbydate_table17_21`
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

#get trackUri from gcp
def get_progress_from_gcs():
    # try reload progress from GCS
    client = get_storage_client(CREDENTIAL_PATH)

    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/test_track_audio_analysis_progress17_21.json")

    if blob.exists():
        progress = json.loads(blob.download_as_text())

    if isinstance(progress, dict):
        trackAudioAnalysis_list = progress["trackAudioAnalysis_list"]
        last_track_uri = progress["last_track_uri"]

        # get track URI from BigQuery
        df = get_track_uris()
        track_uris_bq = list(set(df['trackUri']))  # distinct TrackUri
        logging.info(f"Totall {len(track_uris_bq)} data!")

        # filter track_uris list
        track_uris = filter_track_uris(track_uris_bq, last_track_uri)
        logging.info(f"Still have {len(track_uris)} data!")

        return trackAudioAnalysis_list, track_uris, last_track_uri

    else:
        trackAudioAnalysis_list = progress
        last_track_uri = None
        track_uris = None
        return trackAudioAnalysis_list, track_uris, last_track_uri

    

# def group1(track_uris):
#     track_uris1 = track_uris[0:1201]
#     logging.info("group1")
#     return track_uris1
# def group2(track_uris):
#     track_uris2 = track_uris[1201:2401]
#     logging.info("group2")
#     return track_uris2
# def group3(track_uris):
#     track_uris3 = track_uris[2401:3601]
#     logging.info("group3")
#     return track_uris3
# def group4(track_uris):
#     track_uris4 = track_uris[3601:4801]
#     logging.info("group4")
#     return track_uris4
# def group5(track_uris):
#     track_uris5 = track_uris[4801:6001]
#     logging.info("group5")
#     return track_uris5
# def group6(track_uris):
#     track_uris6 = track_uris[6001:]
#     logging.info("group6")
#     return track_uris6    

def check_if_need_update_token(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for data1
def check_if_need_update_token1(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp1()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp1()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for data2
def check_if_need_update_token2(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp2()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp2()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for data3
def check_if_need_update_token3(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp3()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp3()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for data4
def check_if_need_update_token4(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp4()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp4()

    context['ti'].xcom_push(key='access_token', value=access_token)

#only for data5
def check_if_need_update_token5(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp5()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp5()

    context['ti'].xcom_push(key='access_token', value=access_token)

# store progress data in gcs 
def save_progress_to_gcs(client, progress):
    
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/test_track_audio_analysis_progress17_21.json")
    blob.upload_from_string(json.dumps(progress), content_type='application/json')

## Read progress from gcs track_uris1
def get_track_audio_analysis_data1(**context):
    #change sp token
    access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token1', key='access_token')
    
    trackAudioAnalysis_list, track_uris, last_track_uri = get_progress_from_gcs()
    # df = get_track_uris()
    # track_uris= list(set(df['trackUri']))
    # track_uris1 = group1(track_uris)

    #trackAudioAnalysis_list = []
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

        n = random.randint(1,5) ## gen 1~5s
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
    # progress = {
    #             "last_track_uri": track_uris1[-1],
    #             "trackAudioAnalysis_list": trackAudioAnalysis_list
    #             }

    save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS

## Read progress from gcs1
def get_track_audio_analysis_data2(**context):
    
    trackAudioAnalysis_list, track_uris, last_track_uri = get_progress_from_gcs()
    # track_uris2 = group2(track_uris)

    if last_track_uri is None:
        logging.info("It is the whole data!")

    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token2', key='access_token')

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

                save_progress_to_gcs(client, progress)
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

            n = random.randint(1,5) ## gen 1~5s
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

        logging.info(f"If you see this, means you get group2 {len(trackAudioAnalysis_list)} data!")
        # progress = {
        #         "last_track_uri": track_uris2[-1],
        #         "trackAudioAnalysis_list": trackAudioAnalysis_list
        #         }

        save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS

## Read progress from gcs2
def get_track_audio_analysis_data3(**context):
    
    trackAudioAnalysis_list, track_uris, last_track_uri = get_progress_from_gcs()
    # track_uris3 = group3(track_uris)

    if last_track_uri is None:
        logging.info("It is the whole data!")
    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token3', key='access_token')

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
                time.sleep(30)

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

            n = random.randint(1,5) ## gen 1~5s
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

        logging.info(f"If you see this, means you get group3 {len(trackAudioAnalysis_list)} data")
        # progress = {
        #         "last_track_uri": track_uris3[-1],
        #         "trackAudioAnalysis_list": trackAudioAnalysis_list
        #         }

        save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS

## Read progress from gcs3
def get_track_audio_analysis_data4(**context):
    
    trackAudioAnalysis_list, track_uris, last_track_uri = get_progress_from_gcs()
    # track_uris4 = group4(track_uris)

    if last_track_uri is None:
        logging.info("It is the whole data!")
    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token4', key='access_token')

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
                time.sleep(30)

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

            n = random.randint(1,5) ## gen 1~5s
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

        logging.info(f"If you see this, means you get group4 {len(trackAudioAnalysis_list)} data!")
        # progress = {
        #         "last_track_uri": track_uris4[-1],
        #         "trackAudioAnalysis_list": trackAudioAnalysis_list
        # }
        save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS

## Read progress from gcs4
def get_track_audio_analysis_data5(**context):
    
    trackAudioAnalysis_list, track_uris, last_track_uri = get_progress_from_gcs()
    # track_uris5 = group5(track_uris)

    if last_track_uri == None:
        logging.info("It is the whole data!")
    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token5', key='access_token')

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
                time.sleep(30)

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

            # n = random.randint(1,5) ## gen 1~5s
            # time.sleep(n)

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

        logging.info(f"If you see this, means you get group5 {len(trackAudioAnalysis_list)} data")
        # progress = {
        #         "last_track_uri": track_uris5[-1],
        #         "trackAudioAnalysis_list": trackAudioAnalysis_list
        # }
        save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS

def get_track_audio_analysis_data6(**context):
    
    trackAudioAnalysis_list, track_uris, last_track_uri = get_progress_from_gcs()
    # track_uris6 = group6(track_uris)

    if last_track_uri is None:
        logging.info("It is the whole data!")
    else:
        #change sp token
        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token', key='access_token')

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
                time.sleep(30)

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

            # n = random.randint(1,5) ## gen 1~5s
            # time.sleep(n)

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

        logging.info(f"If you see this, means you get group6 {len(trackAudioAnalysis_list)} data")
        # progress = {
        #         "last_track_uri": track_uris6[-1],
        #         "trackAudioAnalysis_list": trackAudioAnalysis_list
        # }
        save_progress_to_gcs(client, trackAudioAnalysis_list)  # save progress to GCS



def process_data_in_gcs():
    
    client = get_storage_client(CREDENTIAL_PATH)
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob("process/test_track_audio_analysis_progress17_21.json")

    trackAudioAnalysis_list = json.loads(blob.download_as_text())
    # trackAudioAnalysis_list = final_from_gcs["trackAudioAnalysis_list"]
    
    #getTrackUri
    df = get_track_uris()  # from BigQuery
    track_uris = list(set(df['trackUri'])) #distinct TrackUri

    #Extend data
    df_trackUri = pd.DataFrame(track_uris,columns=['trackUri'])
    df_trackAudioAnalysis = pd.DataFrame(trackAudioAnalysis_list)
    df_concat = pd.concat([df_trackUri,df_trackAudioAnalysis], axis=1)
    # df_trackOfAudioAnalysis = pd.concat([pd.DataFrame(df_concat['trackUri']),pd.DataFrame(pd.json_normalize(df_concat['track']))],axis=1)
    
    #Upload to GCS
    local_file_path = 'test_trackOfAudioAnalysis17_21.csv'
    gcs_bucket = 'api_spotify_artists_tracks'
    gcs_file_name = f'output/{local_file_path}'

    df_concat.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)


with DAG('test.py',
        default_args=default_args,
        schedule="@monthly",  
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

    check_if_need_update_token4 = PythonOperator(
        task_id='check_if_need_update_token4',
        python_callable=check_if_need_update_token4,
        provide_context=True,
    )
    check_if_need_update_token5 = PythonOperator(
        task_id='check_if_need_update_token5',
        python_callable=check_if_need_update_token5,
        provide_context=True,
    )

    get_track_audio_analysis_data1 = PythonOperator(
        task_id='get_track_audio_analysis_data1',
        python_callable=get_track_audio_analysis_data1,
        provide_context=True,
    )

    get_track_audio_analysis_data2 = PythonOperator(
        task_id='get_track_audio_analysis_data2',
        python_callable=get_track_audio_analysis_data2,
        provide_context=True,
    )

    get_track_audio_analysis_data3 = PythonOperator(
        task_id='get_track_audio_analysis_data3',
        python_callable=get_track_audio_analysis_data3,
        provide_context=True,
    )

    get_track_audio_analysis_data4 = PythonOperator(
        task_id='get_track_audio_analysis_data4',
        python_callable=get_track_audio_analysis_data4,
        provide_context=True,
    )

    get_track_audio_analysis_data5 = PythonOperator(
        task_id='get_track_audio_analysis_data5',
        python_callable=get_track_audio_analysis_data5,
        provide_context=True,
    )
    get_track_audio_analysis_data6 = PythonOperator(
        task_id='get_track_audio_analysis_data6',
        python_callable=get_track_audio_analysis_data6,
        provide_context=True,
    )

    process_data_in_gcs = PythonOperator(
        task_id='process_data_in_gcs',
        python_callable=process_data_in_gcs,
        provide_context=True,
    )


# Order of DAGs
check_if_need_update_token1 >> get_track_audio_analysis_data1 >> check_if_need_update_token2 >> get_track_audio_analysis_data2 >> check_if_need_update_token3 >> get_track_audio_analysis_data3 >> check_if_need_update_token4 >> get_track_audio_analysis_data4 >> check_if_need_update_token5 >> get_track_audio_analysis_data5 >> check_if_need_update_token >> get_track_audio_analysis_data6 

check_if_need_update_token2.trigger_rule = TriggerRule.ALL_DONE
check_if_need_update_token3.trigger_rule = TriggerRule.ALL_DONE
check_if_need_update_token4.trigger_rule = TriggerRule.ALL_DONE
check_if_need_update_token5.trigger_rule = TriggerRule.ALL_DONE
check_if_need_update_token.trigger_rule = TriggerRule.ALL_DONE


#all task 執行完成才會執行store_data_in_gcs
process_data_in_gcs.set_upstream(
    [get_track_audio_analysis_data1, get_track_audio_analysis_data2, get_track_audio_analysis_data3,
     get_track_audio_analysis_data4, get_track_audio_analysis_data5, get_track_audio_analysis_data6]
)
process_data_in_gcs.trigger_rule = TriggerRule.ALL_DONE