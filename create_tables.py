from ingestion.utils.config import load_config
from ingestion.utils.bigquery import initialize_bigquery_client
import sys


ENV_NAMES = ["dev", "prod"]
TABLE_NAMES = ["citibike_stations"]

def run():
    if len(sys.argv) < 2 or not sys.argv[1] in ENV_NAMES:
        raise Exception("Usage: python create_tables.py <dev|prod>")
    else:
        env_name = sys.argv[1]
    
    config = load_config(env_name)
    dataset_name = config.get("BQ_DATASET_RAW")
    project_id = config.get("GCP_PROJECT_ID")

    client = initialize_bigquery_client(config)

    if not dataset_name or not project_id:
        raise Exception(f"Config missing values for BQ_DATASET_RAW or GCP_PROJECT_ID. Config = {config}")

    for table_name in TABLE_NAMES:
        print(f"Creating table `{project_id}.{dataset_name}.{table_name}` in env {env_name}")
        template_file = f"sql/ddl/templates/raw/{table_name}.sql"
        
        with open(template_file, "r") as f_in:
            template_string = f_in.read()
        
        output_str = template_string.format(
            project_id=project_id,
            dataset_name=dataset_name
        )
    
        print("The following SQL will be run on BigQuery:")
        for line in output_str.split("\n"):
            print(line)
        
        print("Does this look right? Type y to proceed, any other key to abort.")
        proceed = input()

        if proceed != 'y':
            print("Gotcha. Better safe than sorry!")
            return # abort the entire script
        
        else:
            try:
                job = client.query(output_str)
                job.result() # Wait for completion, raise exception if failed
                print(f"Successfully created table {table_name}")
            
            except Exception as e:
                print(f"Failed to create table {table_name}: {e}")


if __name__ == "__main__":
    run()


