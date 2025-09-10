# Citibike Data Pipeline

A comprehensive data pipeline that ingests, transforms, and analyzes NYC Citibike trip and station data to provide insights into bike sharing patterns, commuter behavior, and network performance.

## What This Pipeline Does

This pipeline processes historical and real-time Citibike data to create:
- **Trip Analytics**: Detailed trip patterns, duration analysis, and usage trends
- **Station Performance**: Station popularity, capacity utilization, and geographic insights
- **Network Analysis**: Commuter flow patterns and critical / bottleneck hub identification. [Read more about this analysis here.](./network_analysis.md)
- **Dimensional Models**: Time-based and station-based dimensions for analysis

## Architecture

- **Data Warehouse**: Google BigQuery (bronze/silver/gold medallion architecture)
- **Transformations**: dbt (staging → silver → gold layers)
- **Orchestration**: Apache Airflow DAGs
- **Ingestion**: Custom Python modules for trips, stations, and boundary data
- **Package Structure**: Modular Python package with separate modules for ingestion, database operations, and utilities

## Project Structure

- `citibike/` - Core Python package with ingestion, database, and utility modules
- `dbt_transformations/` - dbt models organized in staging/silver/gold layers
- `airflow/` - Orchestration scripts (coming soon: proper Airflow DAGs)
- `config/` - Environment configuration files
- Root scripts - Setup utilities for tables, profiles, and holidays

## Setup

### Prerequisites
- Python 3.8+
- Google Cloud Platform account

### 1. GCP Account and Project Setup

1. **Create GCP Project**
   - Go to [Google Cloud Console](https://console.cloud.google.com)
   - Create new project called `citibike-pipeline` (note the Project ID)

2. **Enable APIs**
   - BigQuery API
   - Cloud Resource Manager API

3. **Create BigQuery Datasets**
   Create these 2 datasets in BigQuery:
   - `citibike` (production)
   - `citibike_dev` (dev)
   
   Use Multi-region US location for all datasets.

4. **Create Service Account**
   - Go to IAM & Admin > Service Accounts
   - Create service account named `citibike-pipeline-sa`
   - Assign roles: `BigQuery Data Editor`, `BigQuery Job User`
   - Generate and download JSON key

### 2. Local Environment Setup

1. **Create Virtual Environment**
   1. Clone this repo
   2. `cd` into the root directory `citibike-pipeline`
   3. Create and activate a virtual environment
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install Dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   pip install -e . # Install project package
   ```

3. **Configure Environment Files**
   - Copy `config/dev.env.example` to `config/dev.env`
   - Copy `config/prod.env.example` to `config/prod.env`
   - Update `GCP_PROJECT_ID` in both files with your actual project ID
   - Save the JSON key to the path `config/service-account.json`
   - The `GOOGLE_APPLICATION_CREDENTIALS` path is already set to `config/service-account.json` (relative path) in the config files; leave it as is.

### 3. Data Setup

1. **Create BigQuery Tables**
   - Navigate to the project root
   - Run `python create_tables.py dev` to create tables in `dev` environment
   - Run `python create_tables.py prod` to create tables in `prod` environment
   - This creates the raw layer tables for data ingestion
   - Go to the BigQuery console and confirm that the expected tables were created

2. **Configure DBT**
   - Run `python generate_dbt_profile.py` to create `dbt_transformations/profiles.yml`
   - Test connections:
     ```bash
     cd dbt_transformations
     dbt debug # tests dev environment
     dbt debug --target prod # tests prod environment
     ```

3. **Load Seed Data**
   - NYC holidays: `cd dbt_transformations && dbt seed`
   - Borough boundaries: `cd airflow/dags && python boundaries_pipeline.py`

## Running the pipelines

1. **Monthly trips pipeline**
Citibike updates its trip data once per month. This pipeline extracts, ingests, and enriches the trips data all the from raw data, to silver tables usable for AI/ML operations, to the data warehouse dimension tables and custom dashboard reports. It also queries the Citibike GBFS API for the latest stations data as well.

   - `cd` to the `airflow/dags` directory
   - Run the pipeline with the year and month of data you want to process: `python trip_pipeline.py <YYYY> <M>`. For example, for June 2025, run `python trip_pipeline.py 2025 6`.

2. **Network flow analysis pipeline**
This is a pipeline that does more advanced network flow analysis of the silver trips data, resulting in gold layer tables suitable for dashboard visualizations. It produces tables showing the edges and nodes of the morning commuter network, based on trips activity from the last 90 days of data, as well as a table that lists critical and bottleneck stations in the commuter network, ranked by the station's PageRank score.

   - Ensure that you have previously processed trips and stations data (using the monthly trips pipeline) for the most recent 90 days of available data
   - `cd` to the `airflow/dags` directory
   - Run the pipeline: `python network_analysis_pipeline.py`.

## Development and Testing

### Dry Run Mode

For testing configuration and validating connections without executing actual operations, set the `CITIBIKE_DRY_RUN` environment variable:

```bash
export CITIBIKE_DRY_RUN=true
python create_tables.py dev          # Validates config, shows table count, no BigQuery operations
python trip_pipeline.py 2024 6      # Validates config and connections, no data ingestion
python network_analysis_pipeline.py # Validates config, no transformations
```

This mode is useful for:
- Testing configuration changes
- Validating credentials and BigQuery connections
- CI/CD pipeline testing
- Onboarding new developers

### Local Airflow Development

For local development with Airflow, use Docker Compose to run a complete Airflow environment:

```bash
# Start Airflow services (webserver, scheduler, database)
AIRFLOW_UID=$(id -u) docker-compose up

# Access Airflow UI
open http://localhost:8080
# Login: airflow / airflow
```

This provides:
- Airflow webserver at localhost:8080
- Your citibike package and DAGs available in Airflow
- dbt profile automatically generated on container startup
- Config files mounted from your local `config/` directory

To stop the services:
```bash
docker-compose down
```

## Coming soon

- Dashboard reports and visualizations in Looker
- Dockerized Airflow DAGs for orchestrating the pipeline
- Data quality validation and testing stages at key points in the pipeline




