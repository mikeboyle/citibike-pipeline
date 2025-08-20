# Citibike Data Pipeline

A modern data stack implementation using Citibike trip and station data.

## Architecture

- **Orchestration**: Apache Airflow (Docker)
- **Transformations**: dbt
- **Data Warehouse**: Google BigQuery
- **Visualization**: Omni Analytics
- **Ingestion**: Custom Python scripts

## Project Structure

- `airflow/` - Orchestration DAGs and plugins
- `dbt/` - Data transformation models
- `ingestion/` - Python scripts for data loading
- `config/` - Environment configuration
- `sql/` - Templates for creating initial tables in BigQuery

## Setup

### Prerequisites

- Python 3.8+
- Google Cloud Platform account
- Docker (for Airflow)

### 1. GCP Setup

1. **Create GCP Project**
   - Go to [Google Cloud Console](https://console.cloud.google.com)
   - Create new project called `citibike-pipeline` (note the Project ID)

2. **Enable APIs**
   - BigQuery API
   - Cloud Resource Manager API

3. **Create BigQuery Datasets**
   Create these 6 datasets in BigQuery:
   - `raw_dev`, `silver_dev`, `gold_dev` (dev)
   - `raw`, `silver`, `gold` (production)
   
   Use Multi-region US location for all datasets.

4. **Create Service Account**
   - Go to IAM & Admin > Service Accounts
   - Create service account named `citibike-pipeline-sa`
   - Assign roles: `BigQuery Data Editor`, `BigQuery Job User`
   - Generate and download JSON key

5. **Save Credentials**
   - Save the JSON key as `config/service-account.json`

### 2. Local Environment Setup

1. **Create Virtual Environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install Dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. **Configure Environment-Dependent Parameters**
   - Copy `config/dev.env.example` to `config/dev.env`
   - Update `GCP_PROJECT_ID` with your actual project ID

### 3. Create BigQuery tables

1. **Create Raw Tables**
   - Navigate to the project root
   - Run `python create_tables.py dev` to create tables in `dev` environment
   - Run `python create_tables.py prod` to create tables in `prod` environment
   - This creates the raw layer tables for data ingestion 

2. **Test Setup**
   Run this code in your terminal:
   ```bash
   set -a && source config/dev.env && set +a
   python3 -c "
   from google.cloud import bigquery
   import os
   client = bigquery.Client(project=os.environ['GCP_PROJECT_ID'])
   datasets = [d.dataset_id for d in client.list_datasets()]
   print('Connected! Datasets:', datasets)
   "
   ```
   You should see the list of datasets you created in BigQuery.

### 4. Next Steps
TODO: Add instructions for ingestion scripts, dbt models, and Airflow setup as they are built.

