import requests
import base64
import time
from utils.GCP_client import get_bq_client
import os
import logging
from google.cloud import bigquery


def get_latest_token(current_worker: str) -> None:
    client = get_bq_client()
    with client:
        query = f"""
            SELECT *
            FROM affable-hydra-422306-r3.worker.{current_worker}
            ORDER BY access_last_update DESC LIMIT 1
        """
        query_job = client.query(query)
        rows = query_job.result()
        logging.info("Fetching latest token from BigQuery...")
        # access_token, access_last_update, refresh_token, refresh_last_update
        for row in rows:
            # print(type(row))
            # 返回第一行
            return row
        # 如果沒有行數據，返回 None 或者其他適當的值
        return None

# # get current workers' refresh token from GCP
# def get_latest_refresh_token(current_worker):
#     client = get_bq_client(CREDENTIAL_PATH)
#     with client:
#         query = f"""
#         SELECT *
#         FROM affable-hydra-422306-r3.worker.{current_worker}
#         ORDER BY refresh_last_update DESC LIMIT 1
#         """
#         query_job = client.query(query)
#         rows = query_job.result()
#         logging.info("Fetching latest refresh token from BigQuery...")
#         # access_token, access_last_update, refresh_token, refresh_last_update
#         for row in rows:
#             # 返回第一行
#             return row
#         # 如果沒有行數據，返回 None 或者其他適當的值
#         return None


def request_new_ac_token_refresh_token(current_worker: str,
                                       client_id: str,
                                       client_secret: str) -> str:
    '''
    request spotify token
    '''
    refresh_token = get_latest_token(current_worker)[2]
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(
        credentials.encode('utf-8')).decode('utf-8')

    data = {
        'client_id': client_id,
        'grant_type': 'refresh_token',
        'refresh_token': f'{refresh_token}'
    }
    headers = {'content-type': 'application/x-www-form-urlencoded',
               'Authorization': f"Basic {encoded_credentials}",
               }
    response = requests.post(
        'https://accounts.spotify.com/api/token', data=data, headers=headers, timeout=10)
    logging.info(response.text)
    access_token = response.json()['access_token']

    if 'refresh_token' not in response.json():
        refresh_token = refresh_token
    else:
        refresh_token = response.json()['refresh_token']

    client = get_bq_client()
    current_timestamp = int(time.time())
    try:
        query = f"""
            INSERT INTO affable-hydra-422306-r3.worker.{current_worker} (access_token, access_last_update, refresh_token, refresh_last_update)
            VALUES ('{access_token}', {current_timestamp},
                    '{refresh_token}', {current_timestamp})
        """
        client.query(query, location='EU')
        logging.info(f"Token successfully updated: {access_token}")
        return access_token

    except Exception as e:
        logging.info(f"Exception occur: {e}")


def get_workers() -> dict:

    workers = {
        "worker1": {"client_id": "c6c8f465c2d04e83b1a4df4e291ab7e4", "client_secret": "a62d93d1a43a420f8db1214c5d3b6525"},
        "worker2": {"client_id": "338fb263b36446ae995644dffd71e90a", "client_secret": "e7b130133a6549248bbe45beb77bd873"},
        "worker3": {"client_id": "c80c45ec9ef44973a69110fe1a8d6a49", "client_secret": "6c9c7c7329d346f989a150e515109800"},
        "worker4": {"client_id": "5d74f47b9b4a47409290e76895c6dc5c", "client_secret": "a317197c42d547d19f5633b99e9291f9"},
        "worker5": {"client_id": "0e544631eee04591b32066e73c7c165f", "client_secret": "b662d29617f44474832d7c3f08fa679b"},
        "worker6": {"client_id": "5c21b256144d4ab9ae0aad7a54395304", "client_secret": "2d243618ab72493094d24c6c1073acc3"},
        "worker7": {"client_id": "67582a48c7e640de802ff8f1dc6faa4c", "client_secret": "e86094049a3340d3acda9bea6cf0a825"},
        "worker8": {"client_id": "51ee97c3634d49a6bec83fbd22973e8b", "client_secret": "6770be92450f477e99488b2ddeb8ac1d"},
        "worker9": {"client_id": "71328da87c41450fa46d97c7482df1f4", "client_secret": "cc60d88fcdbc4f95a72a25fbca90fd79"},
        "worker10": {"client_id": "cfc9d87588e443cbab833208c17dd73c", "client_secret": "56c4d80ed23f45d0a2a3c79cf3cfa18c"}
    }
    return workers


def check_if_need_update_token(current_worker_name: str, current_worker: str) -> str:
    '''
    check if it update spotify token
    '''
    current_time = int(time.time())
    logging.info(f"Current worker: {current_worker_name}")

    client_id = current_worker["client_id"]
    secret = current_worker["client_secret"]
    access_last_update = get_latest_token(current_worker_name)[1]
    access_token = get_latest_token(current_worker_name)[0]

    logging.info(f"using access_token: {access_token}")

    # means token has been expired, default is 3600, make it 3500 to ensure we have enough time
    if access_last_update + 3500 <= current_time:
        # logging.info(f"access_last_update:{access_last_update} +3500 = {access_last_update+3500} current_time:{current_time}")
        access_token = request_new_ac_token_refresh_token(current_worker_name,
                                                          client_id, secret)

    return access_token
