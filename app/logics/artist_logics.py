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


def get_artists_number():
    """
    get artist number from Schema_Use_Final.dim_Artist
    """
    client = get_bq_client()
    query = """
    SELECT COUNT(DISTINCT(trackMetadata_artists_spotifyUri)) as count
    FROM `affable-hydra-422306-r3.Schema_Use_Final.dim_Artist`
    """

    query_job = client.query(query)
    results = query_job.result()
    # 將結果轉換為字典
    result_dict = {}
    for row in results:
        result_dict["count"] = row["count"]

    return result_dict


def get_artists_detail(artist_id: str):
    """
    get specific artist detail from Schema_Use_Final.dim_Artist
    """
    client = get_bq_client()

    without_spaces = artist_id.replace(' ', '')
    
    query = """
    SELECT *
    FROM `affable-hydra-422306-r3.Schema_Use_Final.dim_Artist`
    WHERE trackMetadata_artists_spotifyUri = @artist_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("artist_id", "STRING", without_spaces)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    # 取得第一行結果，轉換為字典
    artist_detail = dict(next(results))
    return artist_detail
