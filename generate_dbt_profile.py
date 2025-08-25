import yaml
import sys

from citibike.config import load_config

def generate_profile():
    try:
        dev_config = load_config('dev')
        prod_config = load_config('prod')
    except Exception as e:
        print(f"Error loading config: {e}")
        print("Make sure both config/dev.env and config/prod.env exist")
        sys.exit(1)
    
    profile = {
        'dbt_transformations': {
            'outputs': {
                'dev': {
                    'dataset': dev_config.get('BQ_DATASET', 'citibike_dev'),
                    'job_execution_timeout_seconds': 300,
                    'job_retries': 1,
                    'keyfile': dev_config['GOOGLE_APPLICATION_CREDENTIALS'],
                    'location': 'US',
                    'method': 'service-account',
                    'priority': 'interactive',
                    'project': dev_config['GCP_PROJECT_ID'],
                    'threads': 4,
                    'type': 'bigquery',
                },
                'prod': {
                    'dataset': prod_config.get('BQ_DATASET', 'citibike'),
                    'job_execution_timeout_seconds': 600,  # Longer timeout in prod
                    'job_retries': 1,
                    'keyfile': prod_config['GOOGLE_APPLICATION_CREDENTIALS'],
                    'location': 'US',
                    'method': 'service-account', 
                    'priority': 'interactive',
                    'project': prod_config['GCP_PROJECT_ID'],
                    'threads': 8,  # More parallelism in prod
                    'type': 'bigquery',
                }
            },
            'target': 'dev',  # Default to dev for safety
        }
    }
    
    # Write to dbt_transformations/profiles.yml
    with open('dbt_transformations/profiles.yml', 'w') as f:
        yaml.dump(profile, f, default_flow_style=False)
    
    print("Generated dbt_transformations/profiles.yml with dev and prod targets")
    print("Usage:")
    print("\tcd dbt_transformations")
    print("\tdbt debug  # Uses dev (default)")
    print("\tdbt debug --target prod  # Uses prod")

if __name__ == '__main__':
    generate_profile()