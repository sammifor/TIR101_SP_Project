from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from utils.DiscordNotifier import DiscordNotifier
import pandas as pd
from utils.GCP_client import get_bq_client, get_storage_client,load_gcs_to_bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    # 'email': ['a1752815@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_success': True
    'on_failure_callback': DiscordNotifier(msg=" ⚠️️Task Run Failed!⚠️"),
    'on_success_callback': DiscordNotifier(msg=" ✅️Task Run Success!✅")
}

CREDENTIAL_PATH = "./dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"

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
def save_to_gcs(file_name, progress):
    client = get_storage_client(CREDENTIAL_PATH)

    file_path = f"{file_name}.csv"
    gcs_bucket = "api_spotify_artists_tracks"
    gcs_file_name = f"changeDataType/{file_path}"

    progress.to_csv(file_path, index = False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(file_path)

    logging.info(f"{file_path} save to gcs!")

def merge_csv_files(files):
        dfs = []
        for file in files:
            df = pd.read_csv(file)
            dfs.append(df)
        merged_df = pd.concat(dfs, ignore_index=True)
        return merged_df

def merge_output_gcs():
    # input files
    input_files = ['gs://api_spotify_artists_tracks/changeDataType/2017.csv', 'gs://api_spotify_artists_tracks/changeDataType/2018.csv',
                   'gs://api_spotify_artists_tracks/changeDataType/2019.csv', 'gs://api_spotify_artists_tracks/changeDataType/2020.csv',
                   'gs://api_spotify_artists_tracks/changeDataType/2021.csv', 'gs://api_spotify_artists_tracks/changeDataType/2022.csv',
                   'gs://api_spotify_artists_tracks/changeDataType/2023.csv', 'gs://api_spotify_artists_tracks/changeDataType/2024.csv'] 

    merged_data = merge_csv_files(input_files)
    save_to_gcs(file_name = "expand_table_2017_2024", progress = merged_data)
     
def export_to_BQ():
    
    # schema = [
    #     bigquery.SchemaField("column1", "STRING"),
    #     bigquery.SchemaField("column2", "INTEGER"),
    # ]
    load_gcs_to_bigquery(
        gcs_uri=f"gs://api_spotify_artists_tracks/changeDataType/expand_table_2017_2024.csv",
        dataset_id='stage_ChangeDataType',
        table_id=f'expand_table_2017_2024',
        schema=None,
        skip_rows=1
    )

with DAG('merge_chart_data_to_GCS.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    
    merge_output_gcs = PythonOperator(
        task_id='merge_output_gcs',
        python_callable=merge_output_gcs,
        provide_context=True,
    )

    export_to_BQ = PythonOperator(
        task_id='export_to_BQ',
        python_callable=export_to_BQ,
        provide_context=True,
    )
    
    merge_output_gcs >> export_to_BQ




