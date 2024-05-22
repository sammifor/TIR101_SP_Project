# implement main logics here
from google.cloud import bigquery
from dags.utils.GCP_client import get_bq_client


def get_artists_all():
    """
    get all artists from Schema_Use_Final.dim_Artist
    """
    client = get_bq_client()
    query = """
    SELECT *
    FROM `affable-hydra-422306-r3.Schema_Use_Final.dim_Artist`
    """

    query_job = client.query(query)
    results = query_job.result()
    return [dict(row) for row in results]


def get_artists_detail(artist_id: str):
    """
    get specific artist detail from dwd_metadata.dwd_Artist_Genres
    """
    client = get_bq_client()
    query = """
    SELECT *
    FROM `affable-hydra-422306-r3.dwd_metadata.dwd_Artist_Genres`
    WHERE trackMetadat_artists_spotifyUri = @artist_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("artist_id", "STRING", artist_id)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return [dict(row) for row in results]
