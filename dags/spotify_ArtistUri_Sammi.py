import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import logging
from utils.DiscordNotifier import DiscordNotifier
import time
from refresh_token_gcp_Sammi4 import create_bigquery_client
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from refresh_token_gcp_Sammi4 import get_latest_ac_token_gcp1, request_new_ac_token_refresh_token_gcp1
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

def check_if_need_update_token(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp1()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp1()

    context['ti'].xcom_push(key='access_token', value=access_token)

# get ArtistUri from BigQuery
def get_artist_uris():
    path_credential = "./dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"  
    credentials = service_account.Credentials.from_service_account_file(
        path_credential
    )
    client = bigquery.Client(credentials=credentials)

    # query artistUri
    query = """
    SELECT DISTINCT artists_spotifyUri
    FROM `affable-hydra-422306-r3.ChangeDataType.expand_orderbydate_table17_21`
    """
    df = client.query(query).to_dataframe()  

    return df  # return artistUri

# get_Artist_API
def get_artist_data(**context):
    df = get_artist_uris()  # from BigQuery
    artist_uris = list(set(df['artists_spotifyUri'])) #distinct ArtistUri

    # get Spotify token
    access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token', key='access_token')

    artist_data_list = []
    for artist_uri in artist_uris:
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
        get_artist_url = f"https://api.spotify.com/v1/artists/{artist_uri}"

        response = requests.get(get_artist_url, headers=headers,verify=False)

        while response.status_code == 429:
            logging.info(f"Reach the request limitation, doing time sleep now!")
            time.sleep(30)
            response = requests.get(
                get_artist_url,
                headers=headers,
                verify=False
            )
            
        if response.status_code != 200 and response.status_code != 429:# token expired
            logging.info(f"Request a new token for retry")
            access_token = request_new_ac_token_refresh_token_gcp1()
            response = requests.get(
                get_artist_url,
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        },
        verify=False
            )
            
        artist_data = response.json()
        artist_data_list.append(artist_data)

        n = random.randint(1,3) ## gen 1~3s
        time.sleep(n)

    with open('/opt/airflow/dags/rawdata/artist_data.json','w') as f:
        json.dump(artist_data_list,f)
    

def store_data_in_gcs():
    
    with open('/opt/airflow/dags/rawdata/artist_data.json','r') as f:
        artist_data_list = json.load(f)
    
    df = pd.json_normalize(artist_data_list).drop(columns=['images'])
    df_genres = df.explode('genres')

    #Upload to GCS
    local_file_path = 'artistUri17_21.csv'
    gcs_bucket = 'api_spotify_artists_tracks'
    gcs_file_name = f'output/{local_file_path}'

    df_genres.to_csv(local_file_path, index=False)

    storage_client = storage.Client(
        credentials=service_account.Credentials.from_service_account_file("./dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json")
    )
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)



with DAG('spotify_ArtistUri_Sammi.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    check_if_need_update_token = PythonOperator(
        task_id='check_if_need_update_token',
        python_callable=check_if_need_update_token
    )

    get_artist_data = PythonOperator(
        task_id='get_artist_data',
        python_callable=get_artist_data,
        provide_context=True,
    )

    store_data_in_gcs = PythonOperator(
        task_id='store_data_in_gcs',
        python_callable=store_data_in_gcs,
        provide_context=True,
    )

check_if_need_update_token >> get_artist_data >> store_data_in_gcs
