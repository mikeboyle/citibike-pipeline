import pandas as pd
from google.cloud import bigquery

class StagingTableLoader:
    """
    TODO: Docstring
    """
    def __init__(self, client: bigquery.Client, main_table_id: str, batch_key_column: str, primary_key_column: str, staging_table_suffix: str = "_staging"):
        self.client = client
        self.main_table_id = main_table_id
        self.staging_table_id = f"{main_table_id}{staging_table_suffix}"
        self.batch_key_column = batch_key_column
        self.primary_key_column = primary_key_column
    
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

        This is implemented currently by deleting the batch from the main table, then
        inserting the batch from the staging table. In a future iteration, this will be an
        atomic MERGE INTO operation that dynamically sets all of the column values in the
        destination table. To limit code complexity for the moment, this is currently implemented
        with the delete + insert strategy.
        
        Although this is not an atomic operation, in the context of an ETL pipeline this should 
        be okay for the time being. Reads and writes to the main table do not happen simultaneously
        because they are controlled by the orchestration layer, so no consumer of the table will get
        the wrong information by reading in between the delete and update steps. In addition, if the 
        delete step fails, the error will interrupt code execution and the insert will not be attempted.
        When re-running the operation, the staging table will be overwritten (so no duplicate data),
        the delete step would simply be a no-op (nothing to delete), so a successful insert step would 
        have the correct result.
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
