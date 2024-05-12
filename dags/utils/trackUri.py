from utils.GCP_client import get_bq_client
import pandas as pd
import logging
import os 
CREDENTIAL_PATH = os.environ.get('CREDENTIAL_PATH')

# get TrackUri from BigQuery
def get_track_uris(CREDENTIAL_PATH) -> pd.DataFrame:
    client = get_bq_client(CREDENTIAL_PATH)
    query = """
    SELECT DISTINCT trackUri
    FROM `affable-hydra-422306-r3.stage_ChangeDataType.expand_table_2017_2024`
    """
    df = client.query(query).to_dataframe()
    logging.info(f"There are {len(df)} trackUris!")

    return df

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
