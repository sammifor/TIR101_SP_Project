from utils.GCP_client import get_bq_client
import pandas as pd
import logging
from google.cloud import bigquery


def get_track_uris() -> pd.DataFrame:
    '''
    get TrackUri from BigQuery
    '''
    client = get_bq_client()
    query = """
    SELECT DISTINCT trackUri
    FROM `affable-hydra-422306-r3.stage_ChangeDataType.expand_table_2017_2024`
    LIMIT 100
    """
    df = client.query(query).to_dataframe()
    logging.info(f"There are {len(df)} trackUris!")

    return df


def filter_track_uris(track_uris: list, last_track_uri: str) -> list:
    '''
    filter TrackUri list
    '''
    # Find the index of last_track_uri in track_uris
    if last_track_uri in track_uris:
        last_index = track_uris.index(last_track_uri)
        # Keep only the elements after the last_track_uri
        track_uris = track_uris[last_index + 1:]
        logging.info(f"Start from {last_index} of trackUris!")
    return track_uris


def is_last_uri_last_in_list(track_uris: list, last_track_uri: str) -> bool:
    '''
    Check if last_track_uri is the last item in track_uris
    '''
    return track_uris[-1] == last_track_uri if track_uris else False


def get_artist_uris() -> pd.DataFrame:
    '''
    get ArtistUri from BigQuery
    '''
    client = bigquery.Client()

    # query artistUri
    query = """
    SELECT DISTINCT artists_spotifyUri
    FROM `affable-hydra-422306-r3.ChangeDataType.expand_orderbydate_table17_21`
    """
    df = client.query(query).to_dataframe()

    return df  # return artistUri


def check_missing_data(uri_type: str, data: list) -> bool:
    '''
    if there is no missing data of API will return TRUE
    '''
    if uri_type == 'track':
        chart_uris = len(get_track_uris())

    elif uri_type == 'artist':
        chart_uris = len(get_artist_uris())

    logging.info(
        f"download {len(data)} data from API, orginal data has {chart_uris}")
    return len(data) == len(chart_uris)


def find_missing_data(uri_type: str, data: list) -> list:
    '''
    find missing data
    '''
    if uri_type == 'track':
        chart_uris = get_track_uris().values.tolist()

    elif uri_type == 'artist':
        chart_uris = get_artist_uris().values.tolist()

    diff = [item for item in chart_uris if item not in data]
    return diff
