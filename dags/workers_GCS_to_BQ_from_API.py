from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from utils.DiscordNotifier import DiscordNotifier
import pandas as pd
from utils.GCP_client import (
    load_gcs_to_bigquery_native,
    load_gcs_to_bigquery_external,
)
from google.cloud import bigquery

BUCKET_FILE_PATH = {
    "getTrack": "gs://api_spotify_artists_tracks/output/worker_get_track_progress_1724.json",
    "getTrackAudioAnalysis": "gs://api_spotify_artists_tracks/output/worker_get_track_audio_analysis_progress_1724.json",
    "getTrackFeatures": "gs://api_spotify_artists_tracks/output/worker_get_track_audio_features_progress_1724.json",
    "getArtist": "gs://api_spotify_artists_tracks/output/worker_get_artist_progress_1724.json",
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
    "workers_GCS_to_BQ_from_API.py",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
) as dag:

    export_to_BQ = PythonOperator(
        task_id="export_to_BQ",
        python_callable=export_to_BQ,
        description="This DAG waiting for workers_GetArtist, workers_GetTrack, workers_GetTrackAudioAnalysis, workers_GetTrackAudioFeatures",
        provide_context=True,
    )

    # Define sensors for other DAGs
    wait_for_workers_GetArtist = ExternalTaskSensor(
        task_id="wait_for_workers_GetArtist",
        external_dag_id="workers_GetArtist.py",
        external_task_id="process_data_in_gcs",
        dag=dag,
    )

    wait_for_workers_GetTrack = ExternalTaskSensor(
        task_id="wait_for_workers_GetTrack",
        external_dag_id="workers_GetTrack.py",
        external_task_id="process_data_in_gcs",
        dag=dag,
    )

    wait_for_workers_GetTrackAudioAnalysis = ExternalTaskSensor(
        task_id="wait_for_workers_GetTrackAudioAnalysis",
        external_dag_id="workers_GetTrackAudioAnalysis.py",
        external_task_id="process_data_in_gcs",
        dag=dag,
    )

    wait_for_workers_GetTrackAudioFeatures = ExternalTaskSensor(
        task_id="wait_for_workers_GetTrackAudioFeatures",
        external_dag_id="workers_GetTrackAudioFeatures.py",
        external_task_id="process_data_in_gcs",
        dag=dag,
    )

    # Set task dependencies
    (
        wait_for_workers_GetArtist
        >> wait_for_workers_GetTrack
        >> wait_for_workers_GetTrackAudioAnalysis
        >> wait_for_workers_GetTrackAudioFeatures
        >> export_to_BQ
    )
