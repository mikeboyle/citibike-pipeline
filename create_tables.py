from citibike.config import load_config
from citibike.database.bigquery import initialize_bigquery_client
import sys
from glob import glob
from pathlib import Path

ENV_NAMES = ["dev", "prod"]
TEMPLATE_PATHS = glob("sql/ddl/templates/**/*.sql", recursive=True)
TABLE_NAMES = [Path(path).stem for path in TEMPLATE_PATHS]
TABLE_SUFFIXES = ["", "_staging"]

def populate_create_table_query(template_string: str, 
                                project_id: str,
                                dataset_name: str,
                                suffix: str = ""
    ) -> str:
    output_str = template_string.format(
        project_id=project_id,
        dataset_name=dataset_name,
        suffix=suffix
    )

    return output_str

def run() -> None:
    if len(sys.argv) < 2 or not sys.argv[1] in ENV_NAMES:
        raise SystemExit("Usage: python create_tables.py <dev|prod>")
    else:
        env_name = sys.argv[1]
    
    config = load_config(env_name)
    dataset_name = config.get("BQ_DATASET")
    project_id = config.get("GCP_PROJECT_ID")

    client = initialize_bigquery_client(config)

    if not dataset_name or not project_id:
        raise SystemExit(f"Config missing values for BQ_DATASET or GCP_PROJECT_ID. Config = {config}")

    # Prepare list of tables that will be created
    tables_to_create = [f"`{project_id}.{dataset_name}.{table_name}{suffix}`" for table_name in TABLE_NAMES for suffix in TABLE_SUFFIXES]
    print(f"The following tables will be created in env {env_name}:")
    for table_to_create in tables_to_create:
        print(table_to_create)
    
    print("Enter y to proceed, or enter any other key to abort")
    should_proceed = input()

    if should_proceed != "y":
        print("Aborting process.")
        return
    
    # Actually create the tables
    for i, table_name in enumerate(TABLE_NAMES):
        template_path = TEMPLATE_PATHS[i]
        
        with open(template_path, "r") as f_in:
            template_string = f_in.read()
        
        for suffix in TABLE_SUFFIXES:
            query = populate_create_table_query(template_string, project_id, dataset_name, suffix)

            print("\n========================================")
            print(f"{dataset_name}.{table_name}{suffix}")
            print("========================================\n")
        
            print("The following SQL will be run on BigQuery:")
            for line in query.split("\n"):
                print(line)
            
            print("Does this look right? Enter y to proceed, or enter any other key to skip.")
            proceed = input()

            if proceed != 'y':
                print("Skipping table")
                continue # move on to next table
            
            else:
                try:
                    job = client.query(query)
                    job.result() # Wait for completion, raise exception if failed
                    print(f"Successfully created table {table_name}")
                
                except Exception as e:
                    print(f"Failed to create table {table_name}: {e}")
    
    print("Table creation complete.")


if __name__ == "__main__":
    run()


