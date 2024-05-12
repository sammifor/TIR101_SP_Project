from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import logging
import os
import json

CREDENTIAL_PATH = os.environ.get('CREDENTIAL_PATH')

def get_bq_client(CREDENTIAL_PATH) -> bigquery.Client :
    try:
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIAL_PATH
        )
        return bigquery.Client(credentials=credentials)
    except Exception as e:
        logging.info(f"Error connecting to the BigQuery: {e}")
        return None


def get_storage_client(CREDENTIAL_PATH) -> storage.Client:
    try:
        # Load the service account credentials from the specified file
        credentials = service_account.Credentials.from_service_account_file(CREDENTIAL_PATH)
        
        # Create and return a Cloud Storage client using the credentials
        return storage.Client(credentials=credentials)
    except Exception as e:
        logging.info(f"Error connecting to the GCS: {e}")
        return None

def load_gcs_to_bigquery(gcs_uri, dataset_id, table_id, schema, skip_rows=0):
    bigquery_client = get_bq_client(CREDENTIAL_PATH)

    # bq 先定義config
    job_config = bigquery.LoadJobConfig(
        # schema=schema,
        skip_leading_rows=skip_rows,
        autodetect=True,
    )

    # bq load table from gcs uri
    job = bigquery_client.load_table_from_uri(
        gcs_uri,
        dataset_id + "." + table_id,
        job_config=job_config,
    )  

    logging.info(f"{job.result()} - {dataset_id}.{table_id} Upload done!")

# store progress data in gcs
def save_progress_to_gcs(client, progress, bucket_file_path):   
        bucket = client.bucket("api_spotify_artists_tracks")
        blob = bucket.blob(bucket_file_path)
        blob.upload_from_string(json.dumps(progress), content_type='application/json')
        logging.info("save data to gcs!")