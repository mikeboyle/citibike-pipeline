import os
from google.cloud import bigquery
from google.oauth2 import service_account

def initialize_bigquery_client(validate_connection: bool = False) -> bigquery.Client:
    """Initialize BigQuery client using environment variables
    
    Args:
        validate_connection: If True, test connection with list_datasets() call
        
    Returns:
        bigquery.Client: Initialized BigQuery client
        
    Raises:
        KeyError: If required environment variables are not set
        Exception: If credentials file doesn't exist or connection validation fails
    """
    # Read from environment variables (credentials path should already be absolute)
    project_id = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(
        project=project_id,
        credentials=credentials
    )
    
    # Optionally validate connection with lightweight API call
    if validate_connection:
        try:
            datasets = list(client.list_datasets())
            print(f"✅ BigQuery connection validated - found {len(datasets)} datasets in project {project_id}")
        except Exception as e:
            raise Exception(f"BigQuery connection validation failed: {e}")
    
    return client