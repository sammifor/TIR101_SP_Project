import os
from google.oauth2 import service_account
from google.cloud import bigquery


def es_client():
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials)
    return client


def query_to_df(client, query):
    query = f"""
            {query}
            """
    df = client.query(query).to_dataframe()
    return df
