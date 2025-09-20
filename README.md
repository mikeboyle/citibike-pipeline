# Citibike Data Pipeline

A comprehensive data pipeline that ingests, transforms, and analyzes NYC Citibike trip and station data to provide insights into bike sharing patterns, commuter behavior, and network performance.

## What This Pipeline Does

This pipeline processes historical and real-time Citibike data to create:
- **Trip Analytics**: Detailed trip patterns, duration analysis, and usage trends
- **Station Performance**: Station popularity, capacity utilization, and geographic insights
- **Network Analysis**: Commuter flow patterns and critical / bottleneck hub identification. [Read more about this analysis here.](https://mikesboyle.medium.com/predicting-citibike-bottlenecks-with-max-flow-network-analysis-d52aa1b68013)
- **Dimensional Models**: Time-based and station-based dimensions for analysis

## Architecture

- **Data Warehouse**: Google BigQuery (bronze/silver/gold medallion architecture)
- **Transformations**: dbt (staging → silver → gold layers)
- **Orchestration**: Apache Airflow DAGs
- **Ingestion**: Custom Python modules for trips, stations, and boundary data
- **Analysis**: Custom Python modules for graph analysis of the network
- **Package Structure**: Modular Python package with separate modules for ingestion, database operations, and utilities

## Project Structure

- `citibike/` - Core Python package with ingestion, database, and utility modules
- `dbt_transformations/` - dbt models organized in staging/silver/gold layers
- `dags/` - Airflow dags
- `config/` - Environment configuration files
- `plugins/` - Airflow plugins
- `logs/` - Airflow logs (includes logging from dbt tasks run via Airflow)
- `sql/` - Schema for initial tables not managed by dbt
- Root scripts - Setup utilities for tables and seed data

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
   - Clone this repo
   - `cd` into the root directory `citibike-pipeline`
   - Create and activate a virtual environment
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
   - Set local envvars:
      ```bash
      export CITIBIKE_ENV=dev
      export GCP_PROJECT_ID=<your project id>
      ```
   - Test connections:
     ```bash
     cd dbt_transformations
     dbt debug # tests dev environment
     dbt debug --target prod # tests prod environment
     ```

3. **Load Seed Data**
   - NYC holidays: `cd dbt_transformations && dbt seed`
   - Borough boundaries: `cd airflow/dags && python boundaries_pipeline.py`

### Local Airflow Development

For local development with Airflow, use Docker Compose to run a complete Airflow environment:

```bash
# Start Airflow services (webserver, scheduler, database)
CITIBIKE_ENV=dev AIRFLOW_UID=$(id -u) docker compose up

# Access Airflow UI
open http://localhost:8080
# Login: airflow / airflow
```

To stop the services:
```bash
docker compose down
```

#### Selective volume mapping

In local development, the Airflow container maps the host machine project repo as a volume. This enables live editing. It is important to know that we do not mount the entire project, but only the selected directories that are needed to run airflow dags. At this time, those directories or files are:

- `citibike/` (project package with utility files)
- `config/` (config files and credentials for local development)
- `dags/` (Airflow dags)
- `dbt_transformation/` (dbt models, seeds, config, etc.)
- `logs/` (Airflow logs, including dbt logs)
- `plugins/` (created by Airflow, but gitignored)
- `./setup.py` (needed to install the project local package)

If you create a new top level directory in development and you need that directory to be run by an Airflow container, update the `volumes` section of `docker-compose.yaml` to include that directory, as well as any future `COPY` commands in `Dockerfile`.

### Running the pipelines
Pipelines can be triggered and monitored in the Airflow UI. Note that for all DAGS you may first need to turn on the Pause / Unpause toggle to the left of the DAG name in the Airflow DAGs UI.

The available pipelines are:

1. **`boundaries_pipeline` DAG (ready for use)**

   **What it does**: Adds seed data to the `raw_nyc_borough_boundaries` table and then transforms this to geo polygons in the `silver_nyc_borough_boundaries` table.

   **How to run it**:
      - Find the DAG in the Airflow UI `DAGs` page.
      - Press the "play" button (▶️) to the right to manually trigger it.


2. **`trips_pipeline` DAG (ready for use)**

   **What it does**:
   Citibike updates its trip data once per month. This pipeline extracts, ingests, and enriches the trips data all the from raw data, to silver tables usable for AI/ML operations, to the data warehouse dimension tables and custom dashboard reports. It also queries the Citibike GBFS API for the latest stations data as well.

   **How to run it**:
      - Find the DAG in the Airflow UI `DAGs` page.
      - Press the "play" button (▶️) to the right.
      - This will take you to a Params form. Enter your year (`2019`, `2025`, etc.) and month (`1`, `5`, `11`, etc.)
      - Click `Trigger`
      - You will be taken back to the `DAGs` page.

3. **Network flow analysis pipeline (in progress)**

   **What it does**:
   This is a pipeline that does more advanced network flow analysis of the silver trips data, resulting in gold layer tables suitable for dashboard visualizations. It produces tables showing the edges and nodes of the morning commuter network, based on trips activity from the last 90 days of data, as well as a table that lists critical and bottleneck stations in the commuter network, ranked by the station's PageRank score.

   Before running this pipeline, ensure that you have previously processed trips and stations data (using the monthly trips pipeline) for the most recent 90 days of available data.

   **How to run it**:
      - Find the DAG in the Airflow UI `DAGs` page.
      - Press the "play" button (▶️) to the right to manually trigger it.

#### Monitoring pipeline runs

After you trigger a pipeilne run:

- The datetime of the run will appear in the `DAGs` page under `Latest run`.
- Click on this to see a page that monitors the status of each task in the DAG.

## Coming soon

- Production deployment and CI/CD with Kubernetes and GitHub Actions
- Data quality validation and testing stages at key points in the pipeline




