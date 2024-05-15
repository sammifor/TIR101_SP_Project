import logging
from airflow.decorators import dag, task
from datetime import datetime
from utils.BigQueryClient import es_client
import great_expectations as ge


@dag(schedule=None,
     start_date=datetime(2024, 5, 12)
     )
def check_raw_data_quality_dag():
    years = range(2017, 2025)  # 2017 ~ 2024
    for year in years:
        @task
        def get_data(year=year):
            bq_client = es_client()
            end_date = '0331' if year == 2024 else '1231'
            query = f"""
                    SELECT chart_date, count(chartEntryData) as each_chart_date_chartEntryData, count(trackMetadata) as each_chart_date_trackMetadata
                    FROM `affable-hydra-422306-r3.airflow.raw_data_{year}0101_{year}{end_date}`
                    GROUP BY chart_date
            """
            df = bq_client.query(query).to_dataframe()
            return df

        @task
        def check_raw_data_quality(df, year=year):
            df_ge = ge.from_pandas(df)
            special_years = [2016, 2020]  # Feb. got 29 days in these special years
            if year in special_years:
                df_ge.expect_table_row_count_to_equal(366)
            elif year == 2024:  # Original requested data 2024/01/01 ~ 2024/03/31, expected just 91 records
                df_ge.expect_table_row_count_to_equal(91)
            else:
                df_ge.expect_table_row_count_to_equal(365)
            df_ge.expect_column_to_exist("each_chart_date_chartEntryData")
            df_ge.expect_column_to_exist("each_chart_date_trackMetadata")
            df_ge.expect_column_values_to_be_in_set(column='each_chart_date_chartEntryData', value_set=[200])
            df_ge.expect_column_values_to_be_in_set(column='each_chart_date_trackMetadata', value_set=[200])
            results = df_ge.validate()
            logging.info(f'Results statistics:{results.statistics}')
            logging.info(f'Results results:{results.results}')

        task_get_data = get_data()
        check_raw_data_quality(task_get_data, year)


check_raw_data_quality_dag()
