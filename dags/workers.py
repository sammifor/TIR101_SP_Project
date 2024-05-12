from worker_refresh_token import get_latest_refresh_token, get_latest_ac_token, request_new_ac_token_refresh_token
import itertools
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from pandas import json_normalize
import requests
import json
import time
import random
import itertools
import urllib3
from requests.exceptions import SSLError, ConnectionError
urllib3.disable_warnings()
from utils.workers import get_workers

#
# print(get_latest_refresh_token(current_worker_name))
# print(request_new_ac_token_refresh_token(current_worker_name, current_worker["client_id"], current_worker["client_secret"]))

credentials = service_account.Credentials.from_service_account_file(
    '/Users/howard/new_sp/sp_project/cloud/affable-hydra-422306-r3-48540d47aef8.json')
client = bigquery.Client(credentials=credentials)
query = """
    SELECT DISTINCT trackUri
    FROM `affable-hydra-422306-r3.ChangeDataType.expand_orderbydate_table`
"""
df = client.query(query).to_dataframe()
track_uris = list(set(df['trackUri'])) #distinct TrackUri
track_uris = track_uris[7000:8000]


trackAudioFeatures_list = []
start_time = time.time()
workers = get_workers()
#看不懂
worker_cycle = itertools.cycle(workers.items())
current_worker_name, current_worker = next(worker_cycle)
count = 1

for track_uri in track_uris:
    if count % 100 == 0:
        time.sleep(2)
    current_time = time.time()
    print(f"Current worker: {current_worker_name}")
    client_id = current_worker["client_id"]
    secret = current_worker["client_secret"]
    access_last_update = get_latest_refresh_token(current_worker_name)[1]
    access_token = get_latest_ac_token(current_worker_name)[0]
    print(f"using access_token: {access_token}")
    if access_last_update + 3500 <= current_time: # means token has been expired, default is 3600, make it 3500 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token(current_worker_name,
        current_worker["client_id"], current_worker["client_secret"])
        elapsed_time = current_time - start_time
    if elapsed_time >= 10:
        start_time = current_time
        print("Doing switch worker !!")
        current_worker_name, current_worker = next(worker_cycle)
        time.sleep(1)
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
        response = requests.get(f'https://api.spotify.com/v1/audio-features/{track_uri}', headers=headers, verify=False)
        print(response.json())
        count += 1
        track_data = response.json()
        # n = random.randint(1)  ## gen 1~3s
        trackAudioFeatures_list.append(track_data)
        time.sleep(1)
    except (SSLError, ConnectionError) as e:
        response = requests.get(f'https://api.spotify.com/v1/audio-features/{track_uri}', headers=headers, verify=False)
        print(response.json())
        count += 1
        track_data = response.json()
        # n = random.randint(1)  ## gen 1~3s
        trackAudioFeatures_list.append(track_data)
        time.sleep(1)

with open(f'/Users/howard/new_sp/sp_project/dags/datatrackAudioAnalysis_7000_8000.json', 'w') as f:
    json.dump(trackAudioFeatures_list, f)
