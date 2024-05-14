from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from utils.DiscordNotifier import DiscordNotifier
import pandas as pd
from utils.GCP_client import (
    get_bq_client,
    get_storage_client,
    load_gcs_to_bigquery_native,
    load_gcs_to_bigquery_external,
)
from google.cloud import bigquery

BUCKET_FILE_PATH = {
    "getTrack": "gs://api_spotify_artists_tracks/output/worker_get_track_progress_1724.json",
    "getTrackAudioAnalysis": "gs://api_spotify_artists_tracks/output/worker_get_track_audio_analysis_progress_1724.json",
    "getTrackFeatures": "gs://api_spotify_artists_tracks/output/worker_get_track_audio_features_progress_1724.json",
    "getArtist": "gs://api_spotify_artists_tracks/output/worker_get_artists_progress_1724.json",
}
DATASET_ID_TRACK = "stage_TrackSet"
DATASET_ID_ARTIST = "stage_ArtistSet"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 4, 2),
    # 'email': ['a1752815@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_success': True
    "on_failure_callback": DiscordNotifier(msg=" ⚠️️Task Run Failed!⚠️"),
    "on_success_callback": DiscordNotifier(msg=" ✅️Task Run Success!✅"),
}


def export_to_BQ():
    """
    export gcs data to BigQuery, and parameter define which type of table you want to export
    """
    table_type = "native"
    # schema = [
    #     bigquery.SchemaField("column1", "STRING"),
    #     bigquery.SchemaField("column2", "INTEGER"),
    # ]

    for api_name, gcs_uri in BUCKET_FILE_PATH.items():

        if table_type == "native":
            # Native table
            if api_name == "getArtist":
                load_gcs_to_bigquery_native(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_ARTIST,
                    table_id=f"{api_name}",
                    schema=None,
                    skip_rows=1,
                )
            else:
                load_gcs_to_bigquery_native(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_TRACK,
                    table_id=f"{api_name}",
                    schema=None,
                    skip_rows=1,
                )
        # external table
        elif table_type == "external":
            if api_name == "getArtist":
                load_gcs_to_bigquery_external(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_ARTIST,
                    table_id=f"{api_name}",
                    external_source_format="CSV",
                )
            else:
                load_gcs_to_bigquery_external(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_TRACK,
                    table_id=f"{api_name}",
                    external_source_format="CSV",
                )


with DAG(
    "merge_chart_data_to_GCS.py",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
) as dag:

    # merge_output_gcs = PythonOperator(
    #     task_id='merge_output_gcs',
    #     python_callable=merge_output_gcs,
    #     provide_context=True,
    # )

    export_to_BQ = PythonOperator(
        task_id="export_to_BQ",
        python_callable=export_to_BQ,
        provide_context=True,
    )

    # merge_output_gcs >>
    export_to_BQ
