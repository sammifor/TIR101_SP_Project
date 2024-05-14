import logging
from utils.DiscordNotifier import DiscordNotifier
from utils.spotifyUri import (
    get_track_uris,
    is_last_uri_last_in_list,
    filter_track_uris,
    check_missing_data,
    find_missing_data,
)
from utils.GCP_client import get_storage_client, save_progress_to_gcs
from utils.worker_refresh_token import get_workers, check_if_need_update_token
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
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

BUCKET_FILE_PATH = "process/worker_get_track_audio_features_progress_1724.json"
LOCAL_FILE_PATH = "worker_get_track_audio_features_progress_1724.csv"
API = "https://api.spotify.com/v1/audio-features/{}"
DATA_LIST_NAME = "trackAudioFeatures_list"
URI_TYPE = "track"

# you may change the email to yours, if you want to change the sender's info, you may go config/airflow.cfg replace [smpt] related information.
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 4, 2),
    # 'email': ['a1752815@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_success': True
    "on_failure_callback": DiscordNotifier(msg=" ⚠️️Task Run Failed!⚠️"),
    "on_success_callback": DiscordNotifier(msg=" ✅️Task Run Success!✅"),
}


def for_loop_get_response(
    track_uris: list, last_track_uri: str, trackData_list: list
) -> list:
    """
    for loop to get API response
    """
    start_time = int(time.time())

    workers = get_workers()
    worker_cycle = itertools.cycle(workers.items())
    current_worker_name, current_worker = next(worker_cycle)

    # 是下面的for迴圈 count % 100
    count = 1

    for track_uri in track_uris:

        current_time = int(time.time())
        elapsed_time = current_time - start_time

        if elapsed_time >= 30:
            start_time = current_time
            print(f"{elapsed_time} - Doing switch worker !!")
            current_worker_name, current_worker = next(worker_cycle)
            time.sleep(1)

        if not is_last_uri_last_in_list(track_uris, last_track_uri):
            access_token = check_if_need_update_token(
                current_worker_name, current_worker
            )

            headers = {
                "accept": "*/*",
                "accept-language": "zh-TW,zh;q=0.8",
                "authorization": f"Bearer {access_token}",
                "origin": "https://developer.spotify.com",
                "referer": "https://developer.spotify.com/",
                "sec-ch-ua": '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "sec-gpc": "1",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Connection": "close",
            }

            get_track_url = API.format(track_uri)
            print(get_track_url)

            try:
                response = requests.get(get_track_url, headers=headers, verify=False)

                if response.status_code == 429:
                    logging.info(
                        f"Reach the request limitation, change the worker now!"
                    )
                    time.sleep(10)
                    access_token = check_if_need_update_token(
                        current_worker_name, current_worker
                    )
                    response = requests.get(
                        get_track_url,
                        headers={
                            "accept": "*/*",
                            "accept-language": "zh-TW,zh;q=0.8",
                            "authorization": f"Bearer {access_token}",
                            "origin": "https://developer.spotify.com",
                            "referer": "https://developer.spotify.com/",
                            "sec-ch-ua": '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                            "sec-ch-ua-mobile": "?0",
                            "sec-ch-ua-platform": '"macOS"',
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-site",
                            "sec-gpc": "1",
                            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                            "Connection": "close",
                        },
                        verify=False,
                    )

                track_data = response.json()
                trackData_list.append(track_data)

                count += 1
                logging.info(f"{count}-{track_uri}")

                # n = random.randint(1,3)  ## gen 1~3s
                time.sleep(1)

                # 每100筆睡2秒
                if count % 100 == 0:
                    time.sleep(2)
                    client = get_storage_client()

                    progress = {
                        "last_track_uri": track_uri,
                        DATA_LIST_NAME: trackData_list,
                    }

                    # save progress to GCS
                    save_progress_to_gcs(client, progress, BUCKET_FILE_PATH)

            except (SSLError, ConnectionError) as e:
                response = requests.get(get_track_url, headers=headers, verify=False)
                logging.info(f"get the {e} data again done!")
                
                track_data = response.json()
                trackData_list.append(track_data)

                count += 1
                logging.info(f"{count}-{track_uri}")

                client = get_storage_client()

                progress = {"last_track_uri": track_uri, DATA_LIST_NAME: trackData_list}

                # save progress to GCS
                save_progress_to_gcs(client, progress, BUCKET_FILE_PATH)
                raise AirflowFailException("Connection error, marking DAG as failed.")

    return trackData_list


def get_track_data(**context):
    """
    fetch Spotify Developer API - get Track, this function will push response list result to next dag

    """

    df = get_track_uris()
    track_uris = list(set(df["trackUri"]))  # distinct TrackUri

    # read form gcs
    # try reload progress from GCS
    client = get_storage_client()

    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(BUCKET_FILE_PATH)

    if blob.exists():
        progress = json.loads(blob.download_as_text())
        trackData_list = progress[DATA_LIST_NAME]
        last_track_uri = progress["last_track_uri"]
        track_uris = filter_track_uris(track_uris, last_track_uri)
    else:
        trackData_list = []
        last_track_uri = None

    trackData_list = for_loop_get_response(track_uris, last_track_uri, trackData_list)

    context["ti"].xcom_push(key="result", value=trackData_list)


def check_no_missing_data(**context):
    """
    make sure no missing data from API
    """
    trackData_list = context["ti"].xcom_pull(task_ids="get_track_data", key="result")
    if check_missing_data(URI_TYPE, data=trackData_list):
        client = get_storage_client()
        save_progress_to_gcs(client, trackData_list, BUCKET_FILE_PATH)
        logging.info(
            f"If you see this, means you get the whole data - {len(trackData_list)} from get_track API!"
        )
    else:
        # get API data again and put missing data in trackData_list
        track_uris = find_missing_data(URI_TYPE, data=trackData_list)
        trackData_list = for_loop_get_response(
            track_uris, last_track_uri=None, trackData_list=trackData_list
        )
        logging.info(f"Get the missing data done! There is {len(trackData_list)} data")
        client = get_storage_client()
        save_progress_to_gcs(client, trackData_list, BUCKET_FILE_PATH)


def process_data_in_gcs():

    client = get_storage_client()
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(BUCKET_FILE_PATH)

    trackAudioFeatures_list = json.loads(blob.download_as_text())

    # Extend data
    df_trackAudioFeatures = pd.json_normalize(trackAudioFeatures_list)

    # Upload to GCS
    local_file_path = LOCAL_FILE_PATH
    gcs_bucket = "api_spotify_artists_tracks"
    gcs_file_name = f"output/{local_file_path}"

    df_trackAudioFeatures.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)


with DAG(
    "workers_GetTrack.py",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
) as dag:

    get_track_data = PythonOperator(
        task_id="get_track_data",
        python_callable=get_track_data,
        provide_context=True,
    )

    check_no_missing_data = PythonOperator(
        task_id="check_no_missing_data",
        python_callable=check_no_missing_data,
        provide_context=True,
    )

    process_data_in_gcs = PythonOperator(
        task_id="process_data_in_gcs",
        python_callable=process_data_in_gcs,
        provide_context=True,
    )

# Order of DAGs
get_track_data >> check_no_missing_data >> process_data_in_gcs
