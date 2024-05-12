from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import json
import logging
from utils.DiscordNotifier import DiscordNotifier
import pandas as pd
from google.cloud import bigquery
from utils.GCP_client import get_bq_client, get_storage_client, load_gcs_to_bigquery
import os 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    # 'email': ['a1752815@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_success': True
    'on_failure_callback': DiscordNotifier(msg=" ⚠️️Task Run Failed!⚠️"),
    'on_success_callback': DiscordNotifier(msg=" ✅️Task Run Success!✅")
}

CREDENTIAL_PATH = os.environ.get('CREDENTIAL_PATH')
RAWDATA_2017 = "affable-hydra-422306-r3.airflow.raw_data_20170101_20171231"
RAWDATA_2018 = "affable-hydra-422306-r3.airflow.raw_data_20180101_20181231"
RAWDATA_2019 = "affable-hydra-422306-r3.airflow.raw_data_20190101_20191231"
RAWDATA_2020 = "affable-hydra-422306-r3.airflow.raw_data_20200101_20201231"
RAWDATA_2021 = "affable-hydra-422306-r3.airflow.raw_data_20210101_20211231"
RAWDATA_2022 = "affable-hydra-422306-r3.airflow.raw_data_20220101_20221231"
RAWDATA_2023 = "affable-hydra-422306-r3.airflow.raw_data_20230101_20231231"
RAWDATA_2024 = "affable-hydra-422306-r3.airflow.raw_data_20240101_20240331"
DATASET = { 2017: RAWDATA_2017, 2018: RAWDATA_2018, 2019: RAWDATA_2019, 2020:RAWDATA_2020, 
           2021: RAWDATA_2021, 2022: RAWDATA_2022, 2023: RAWDATA_2023, 2024: RAWDATA_2024 }

#get chart data from bigQuery
def get_chart_data_from_BQ(CREDENTIAL_PATH, dataset) -> pd.DataFrame:     
    client = get_bq_client(CREDENTIAL_PATH)

    # data set location
    query = f"""
    SELECT *
    FROM `{dataset}`
    """

    df = client.query(query).to_dataframe() 
    return df

#save to gcs
def save_to_gcs(year, progress):
    client = get_storage_client(CREDENTIAL_PATH)

    file_path = f"{year}.csv"
    gcs_bucket = "api_spotify_artists_tracks"
    gcs_file_name = f"changeDataType/{file_path}"

    progress.to_csv(file_path, index = False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(file_path)

    logging.info(f"{file_path} save to gcs!")

#clean data
def expand_and_change_datatype():

    for year, dataset in DATASET.items():
        df = get_chart_data_from_BQ(CREDENTIAL_PATH, dataset)
        #補年
        df['chart_date'] = df['chart_date'].apply(lambda x: f"{year}-{x}")
        #整理資料
        expanded_df = pd.json_normalize(df['chartEntryData'].apply(json.loads))
        expanded_df1 = pd.json_normalize(df['trackMetadata'].apply(json.loads))
        result_df = pd.concat([df, expanded_df, expanded_df1], axis=1).drop(columns=['chartEntryData','trackMetadata','producers','songWriters','displayImageUri','rankingMetric.type'])
        
        explode_artists = result_df.explode('artists')
        explode_labels = explode_artists.explode('labels')

        expanded_artists = pd.json_normalize(explode_labels['artists']).drop(columns='externalUrl')
        expanded_artists["artist_name"] = expanded_artists["name"]
        expanded_artists["artistUri"] = expanded_artists["spotifyUri"]
        df_artists = expanded_artists.drop(columns=['name','spotifyUri'])

        expanded_labels = pd.json_normalize(explode_labels['labels']).drop(columns=['spotifyUri','externalUrl'])
        expanded_labels["labels"] = expanded_labels["name"]
        labels_name = expanded_labels.drop(columns='name')

        df_drop = explode_labels.drop(columns= ['artists','labels'])
        df_reset = df_drop.reset_index()

        artists_labels = pd.concat([df_reset,df_artists, labels_name], axis=1)

        save_to_gcs(year, artists_labels)

def export_to_BQ():
    
    for year in range(2017,2025):
        load_gcs_to_bigquery(
            gcs_uri = f'gs://api_spotify_artists_tracks/changeDataType/{year}.csv',
            dataset_id = 'stage_ChangeDataType',
            table_id = f'{year}',
            external_source_format='CSV',
            schema = [
                bigquery.SchemaField("chart_date", "DATE"),
                bigquery.SchemaField("missingRequiredFields", "BOOLEAN"),
                bigquery.SchemaField("update_time", "INTEGER"),
                bigquery.SchemaField("currentRank", "INTEGER"),
                bigquery.SchemaField("previousRank", "INTEGER"),
                bigquery.SchemaField("peakRank", "INTEGER"),
                bigquery.SchemaField("peakDate", "DATE"),
                bigquery.SchemaField("appearancesOnChart", "INTEGER"),
                bigquery.SchemaField("consecutiveAppearancesOnChart", "INTEGER"),
                bigquery.SchemaField("entryStatus", "STRING"),
                bigquery.SchemaField("entryRank", "INTEGER"),
                bigquery.SchemaField("entryDate", "DATE"),
                bigquery.SchemaField("rankingMetric_value", "INTEGER"),
                bigquery.SchemaField("trackName", "STRING"),
                bigquery.SchemaField("trackUri", "STRING"),
                bigquery.SchemaField("releaseDate", "DATE"),
                bigquery.SchemaField("artist_name", "STRING"),
                bigquery.SchemaField("artistUri", "STRING"),
                bigquery.SchemaField("labels", "STRING"),
            ]
        )

with DAG('get_chart_data_from_BQ.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    
    expand_and_change_datatype = PythonOperator(
        task_id='expand_and_change_datatype',
        python_callable=expand_and_change_datatype,
        provide_context=True,
    )

    export_to_BQ = PythonOperator(
        task_id='export_to_BQ',
        python_callable=export_to_BQ,
        provide_context=True,
    )

    # Trigger DAG B after DAG A completes
    trigger_dag_b = TriggerDagRunOperator(
        task_id='trigger_dag_b',
        trigger_dag_id='merge_chart_data_to_GCS.py',  # DAG B name
        conf={},  # 可以添加需要傳遞給 DAG B 的任何參數
        dag=dag
    )

    expand_and_change_datatype >> export_to_BQ >> trigger_dag_b




