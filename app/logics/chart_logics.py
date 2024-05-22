# implement main logics here
from typing import Optional
from google.cloud import bigquery
from dags.utils.GCP_client import get_bq_client


def get_charts(
    year: Optional[int],
    month: Optional[int],
    day: Optional[int],
    rank: Optional[int],
    limit: int,
):
    """
    Get all charts from dwd_metadata.dwd_chart_tracks_artists_genres
    """
    client = get_bq_client()
    query = """
    SELECT *
    FROM `affable-hydra-422306-r3.dwd_metadata.dwd_chart_tracks_artists_genres` AS charts
    WHERE (@year IS NULL OR EXTRACT(YEAR FROM charts.chart_date) = @year)
    AND (@month IS NULL OR EXTRACT(MONTH FROM charts.chart_date) = @month)
    AND (@day IS NULL OR EXTRACT(DAY FROM charts.chart_date) = @day)
    AND (@rank IS NULL OR charts.ChartEntryData_currentRank = @rank)
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year", "INT64", year),
            bigquery.ScalarQueryParameter("month", "INT64", month),
            bigquery.ScalarQueryParameter("day", "INT64", day),
            bigquery.ScalarQueryParameter("rank", "INT64", rank),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    return [dict(row) for row in results]


def get_charts_by_year(year: int):
    """
    get yearly charts from dwd_metadata.dwd_chart_tracks_artists_genres
    """
    client = get_bq_client()
    query = """
    SELECT *
    FROM `affable-hydra-422306-r3.dwd_metadata.dwd_chart_tracks_artists_genres`
    WHERE EXTRACT(YEAR FROM chart_date) = @year
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("year", "INT64", year)]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    # 取得第一行結果，轉換為字典
    yearly_chart = dict(next(results))
    return yearly_chart
