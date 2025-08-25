from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Any, Dict

def initialize_bigquery_client(config: Dict[str, Any]) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(config['GOOGLE_APPLICATION_CREDENTIALS'])
    client = bigquery.Client(
        project=config['GCP_PROJECT_ID'],
        credentials=credentials
    )
    
    return client