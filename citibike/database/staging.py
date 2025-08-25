import pandas as pd
from google.cloud import bigquery

class StagingTableLoader:
    """
    Loads a dataframe to a staging table in big query,
    and then merges the staging data into the main table.
    """
    def __init__(self, client: bigquery.Client, main_table_id: str, batch_key_column: str, staging_table_suffix: str = "_staging"):
        self.client = client
        self.main_table_id = main_table_id
        self.staging_table_id = f"{main_table_id}{staging_table_suffix}"
        self.batch_key_column = batch_key_column
    
    def load_and_merge_df(self, df: pd.DataFrame, batch_key_value: str) -> None:
        self._load_df_to_staging(df)
        self._merge_staging_to_main(batch_key_value)
    
    def _load_df_to_staging(self, df: pd.DataFrame):
        """Load pandas df into staging table, overwriting previous values in staging table"""
        try:
            # Configure job to truncate staging table
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA
            )

            # Load dataframe to staging table
            job = self.client.load_table_from_dataframe(
                dataframe=df,
                destination=self.staging_table_id,
                job_config=job_config
            )

            # Wait for completion
            job.result()

            print(f"Loaded {len(df)} rows to staging table {self.staging_table_id}")

        except Exception as e:
            raise Exception(f"Load operation failed; rerun load_and_merge_df method to retry. Error: {e}")


    def _merge_staging_to_main(self, batch_key_value):
        """
        Replaces the data in the main table with the given batch_key_value with
        the corresponding data in the staging table.
        """
        try:
            # Step 1: Delete existing batch
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("batch_key_value", "STRING", batch_key_value)
                ]
            )
            delete_sql = f"""
            DELETE FROM `{self.main_table_id}`
            WHERE DATE({self.batch_key_column}) = DATE(@batch_key_value);
            """
            job = self.client.query(delete_sql, job_config=job_config)
            job.result()
            print(f"Deleted batch {batch_key_value} from {self.main_table_id}")

            # Step 2: Insert all staging data
            insert_sql = f"""
            INSERT INTO `{self.main_table_id}`
            SELECT * FROM `{self.staging_table_id}`
            """
            self.client.query(insert_sql).result()
            print(f"Inserted new data from batch {batch_key_value} into {self.main_table_id}")

        except Exception as e:
            # Log error, let caller decide whether to retry
            raise Exception(f"Merge operation failed; rerun load_and_merge_* method to retry. Error: {e}")
