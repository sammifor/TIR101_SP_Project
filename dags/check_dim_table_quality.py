import logging
from airflow.decorators import dag, task
from datetime import datetime
from utils.BigQueryClient import es_client, query_to_df



@dag(schedule=None,
     start_date=datetime(2024, 5, 12)
     )
def check_dim_table_quality_dag():
    def set_gx_context():

        from great_expectations.data_context import FileDataContext
        path_to_folder = "/opt/airflow/dags"
        context = FileDataContext.create(project_root_dir=path_to_folder)
        return context

    @task
    def check_dim_table_data():

        project_id = 'affable-hydra-422306-r3'
        dataset_id = 'Schema_Use_Final'
        context = set_gx_context()
        datasource = context.get_datasource("my_pandas_datasource")
        bq_client = es_client()
        track_table_query = f"""select * from `{project_id}.{dataset_id}.dim_Tracks`"""
        track_table_df = query_to_df(bq_client, track_table_query)
        data_asset = datasource.add_dataframe_asset(name="track_table_df")
        track_batch_request = data_asset.build_batch_request(dataframe=track_table_df)
        context.add_or_update_expectation_suite("my_expectation_suite")
        validator = context.get_validator(
            batch_request=track_batch_request,
            expectation_suite_name="my_expectation_suite",
        )
        validator.expect_column_values_to_not_be_null(column="trackMetadata_trackUri", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="track_name", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="track_disc_number", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="track_duration_ms", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="track_explicit", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="track_popularity", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="trackMetadata_artists_spotifyUri", mostly=0.9)
        validator.expect_column_values_to_not_be_null(column="album_id", mostly=0.9)
        validator.expect_table_row_count_to_equal(10052)
        validator.expect_table_column_count_to_equal(8)
        validator.save_expectation_suite(discard_failed_expectations=False)
        checkpoint = context.add_or_update_checkpoint(
            name="my_custom_checkpoint",
            validator=validator,
        )
        checkpoint_result = checkpoint.run()
        context.view_validation_result(checkpoint_result)
        logging.info("Check complete, you can go to folder check the results!")

    @task
    def drop_dfs_from_asset():
        # Drop dataframes to Reuse
        context = set_gx_context()
        datasource = context.get_datasource("my_pandas_datasource")
        datasource_set = datasource.get_asset_names()
        if len(datasource_set) != 0:
            for _ in datasource_set:
                datasource.delete_asset(_)
            logging.info(f"Pandas Data Source Deleted Successfully:{datasource_set}")
        else:
            logging.info(f"Your Pandas Data Source is empty")

    task1 = check_dim_table_data()
    task2 = drop_dfs_from_asset()
    task1 >> task2


check_dim_table_quality_dag()
