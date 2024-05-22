# implement main logics here
from google.cloud import bigquery
from dags.utils.GCP_client import get_bq_client


def get_tracks_all():
    """
    get all tracks from Schema_Use_Final.dim_Tracks
    """
    client = get_bq_client()
    query = """
    SELECT *
    FROM `affable-hydra-422306-r3.Schema_Use_Final.dim_Tracks`
    """

    query_job = client.query(query)
    results = query_job.result()
    return [dict(row) for row in results]


def get_tracks_number():
    """
    get track number from Schema_Use_Final.dim_Tracks
    """
    client = get_bq_client()
    query = """
    SELECT COUNT(DISTINCT(trackMetadata_trackUri)) as count
    FROM `affable-hydra-422306-r3.Schema_Use_Final.dim_Tracks`
    """

    query_job = client.query(query)
    results = query_job.result()
    # 將結果轉換為字典
    result_dict = {}
    for row in results:
        result_dict["count"] = row["count"]

    return result_dict


def get_tracks_detail(track_id: str):
    """
    get specific track detail from dwd_tracks_with_analysis_and_features
    """
    client = get_bq_client()
    query = """
    SELECT trackMetadata_trackUri, track_name, track_duration_ms, track_explicit,
    track_popularity, trackMetadata_artists_spotifyUri, album_id,
    track_end_of_fade_in, track_start_of_fade_out, track_loudness, track_tempo, 
    track_tempo_confidence, acousticness, danceability, energy, instrumentalness, liveness, valence
    FROM `affable-hydra-422306-r3.mart_metadata.dwd_tracks_with_analysis_and_features`
    WHERE trackMetadata_trackUri = @track_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("track_id", "STRING", track_id)]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    # 取得第一行結果，轉換為字典
    track_detail = dict(next(results))
    return track_detail
