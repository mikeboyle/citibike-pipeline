# Citibike Data Pipeline

A comprehensive data pipeline that ingests, transforms, and analyzes NYC Citibike trip and station data to provide insights into bike sharing patterns, commuter behavior, and network performance.

## What This Pipeline Does

This pipeline processes historical and real-time Citibike data to create:
- **Trip Analytics**: Detailed trip patterns, duration analysis, and usage trends
- **Station Performance**: Station popularity, capacity utilization, and geographic insights
- **Network Analysis**: Commuter flow patterns and critical / bottleneck hub identification
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
- `airflow/` - Orchestration DAGs and custom operators
- `config/` - Environment configuration files
- Root scripts - Setup utilities for tables, profiles, and holidays

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
   Create these 2 datasets in BigQuery:
   - `citibike` (production)
   - `citibike_dev` (dev)
   
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
   cd root/dir/of/reop
   pip install --upgrade pip
   pip install -r requirements.txt
   pip install -e . # Install project package
   ```

3. **Configure Environment-Dependent Parameters**
   - Copy `config/dev.env.example` to `config/dev.env`
   - Copy `config/prod.env.example` `config/prod.env`
   - Update `GCP_PROJECT_ID` in both files with your actual project ID
   - Update `GOOGLE_APPLICATION_CREDENTIALS` to the **absolute** path to your `service-account.json` credentials


### 3. Create BigQuery tables

1. **Create Raw Tables**
   - Navigate to the project root
   - Run `python create_tables.py dev` to create tables in `dev` environment
   - Run `python create_tables.py prod` to create tables in `prod` environment
   - This creates the raw layer tables for data ingestion 

2. **Manually verify**
   Go to the BigQuery console and confirm that the expected tables were created with the columns and datatypes you expect.

### 4. Configure DBT
1. **Configure profile**
   - **In the project root**, run `generate_dbt_profile.py`
   - This should create the file `dbt_transformations/profiles.yml`

2. **Test dbt connection to BigQuery**
   - `cd dbt_transformations`
   - `dbt debug` # tests dev environment
   - `dbt debug --target prod` # tests prod environment

### 5. Add seed data
1. **Borough boundaries**
   - `cd` to the `airflow/dags` directory
   - run `python boundaries_pipeline.py`
   - This should populate the `raw_nyc_borough_boundaries` table in BigQuery
   - This should also create and populate the `silver_nyc_borough_boundaries` table in BigQuery

2. **NYC holidays**
   - `cd` to `dbt_transformations` directory
   - run `dbt seed`
   - This should create and populate the `holidays` table in BigQuery
   - Note: It is **not** necessary to run the `generate_holidays.py` script. This has already been done to generate the `holidays.csv` file used as a seed.


