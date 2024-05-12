from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.exceptions import NotFound
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

def load_gcs_to_bigquery(gcs_uri, dataset_id, table_id, external_source_format, schema):

    # Construct a BigQuery client object.
    client = get_bq_client(CREDENTIAL_PATH)

    # Set table_id to the ID of the table to create.
    table_path = f"affable-hydra-422306-r3.{dataset_id}.{table_id}"

    # Create ExternalConfig object with external source format
    external_config = bigquery.ExternalConfig(external_source_format)
    # Set source_uris that point to your data in Google Cloud
    external_config.source_uris = gcs_uri

    # Check if the table exists
    table = bigquery.Table(table_path)
    try:
        client.get_table(table)
        logging.info(f"Table {dataset_id}.{table_id} already exists")
    except NotFound:
        # Dataset doesn't exist, create it
        table.external_data_configuration = external_config
        # Set the external data configuration of the table
        table.external_data_configuration = external_config

        table.schema = schema

        table = client.create_table(table)  # Make an API request.

        logging.info(f"{dataset_id}.{table_id} Upload done!")

# store progress data in gcs
def save_progress_to_gcs(client, progress, bucket_file_path):   
        bucket = client.bucket("api_spotify_artists_tracks")
        blob = bucket.blob(bucket_file_path)
        blob.upload_from_string(json.dumps(progress), content_type='application/json')
        logging.info("save data to gcs!")