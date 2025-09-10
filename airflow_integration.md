# Airflow Integration Progress

## Overview
We're refactoring this project to use Apache Airflow for orchestration, with containerization and secrets management best practices. The environment variable refactor is **COMPLETE** ✅.

## ✅ Completed: Environment Configuration Refactor

### What We Accomplished
- **Centralized environment loading**: `load_env_config()` function loads from `.env` files and resolves credential paths
- **Eliminated config dict dependencies**: All modules now read directly from environment variables
- **Path resolution**: Relative credential paths automatically resolved to absolute paths from project root
- **Enhanced validation**: Dry run mode now includes actual BigQuery connection testing
- **Simplified codebase**: Removed all temporary config dict shims

### Key Changes Made
1. **Updated `citibike/config/__init__.py`**:
   - `load_env_config()` - Loads env vars and resolves credential paths
   - `get_config_value_dict()` - For scripts needing multiple environments (dbt profile generation)

2. **Refactored `citibike/database/bigquery.py`**:
   - `initialize_bigquery_client()` - Reads from env vars, optional connection validation
   - Removed path resolution logic (handled centrally now)

3. **Updated all pipeline scripts**:
   - Removed config dict shims
   - Direct environment variable usage
   - Enhanced dry run validation with BigQuery connection testing

4. **Enhanced dry run mode**:
   - Tests actual BigQuery connectivity via `list_datasets()` API
   - Validates credential files exist and are readable
   - Shows resolved absolute paths for debugging

### Data Migration Completed
- Moved working data from `citibike_dev` → `citibike` (production)
- Updated Looker dashboards to use production dataset
- Ready for testing refactored scripts against fresh `citibike_dev`

## Environment Variable Management

### Local Development
```bash
# Set environment variables
export CITIBIKE_ENV=dev
export CITIBIKE_DRY_RUN=true

# Run pipeline
python airflow/dags/trip_pipeline.py 2024 1
```

**How it works:**
- `load_env_config('dev')` loads `config/dev.env` 
- Relative paths like `credentials/dev-service-account.json` become absolute
- Environment variables available to all modules via `os.environ`

### Docker Container
```dockerfile
# Option 1: Copy config files into container
COPY config/ /app/config/
COPY credentials/ /app/credentials/
ENV CITIBIKE_ENV=prod

# Option 2: Set environment variables directly
ENV GCP_PROJECT_ID=your-project
ENV BQ_DATASET=citibike
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/prod-service-account.json
ENV GBFS_STATION_URL=https://gbfs.citibikenyc.com/gbfs/en/station_information.json
# ... other env vars
```

**How it works:**
- With Option 1: `load_env_config()` finds config files, resolves paths within container
- With Option 2: Skip `load_env_config()`, environment variables already set
- Credential paths resolved to container's filesystem

### Kubernetes Secrets Injection
```yaml
# Option 1: ConfigMap + Secret for credentials
apiVersion: v1
kind: ConfigMap
metadata:
  name: citibike-config
data:
  GCP_PROJECT_ID: "your-project"
  BQ_DATASET: "citibike"
  GBFS_STATION_URL: "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
---
apiVersion: v1
kind: Secret
metadata:
  name: citibike-credentials
type: Opaque
data:
  service-account.json: <base64-encoded-json>

# Pod spec
spec:
  containers:
  - name: citibike-pipeline
    env:
    - name: GCP_PROJECT_ID
      valueFrom:
        configMapKeyRef:
          name: citibike-config
          key: GCP_PROJECT_ID
    - name: GOOGLE_APPLICATION_CREDENTIALS
      value: "/etc/secrets/service-account.json"
    volumeMounts:
    - name: credentials
      mountPath: "/etc/secrets"
      readOnly: true
  volumes:
  - name: credentials
    secret:
      secretName: citibike-credentials
```

**How it works:**
- ConfigMap provides non-sensitive config
- Secret provides service account JSON file
- Environment variables injected directly into pod
- No config files needed - pure environment variable approach

### Testing Status
- ✅ Local dry run testing - All pipelines validated
- ✅ DBT profile generation - Works with resolved paths
- ⏭️ Ready for live testing against fresh dev environment

## Next Steps
1. **Test live execution**: Run small pipeline against fresh `citibike_dev`
  - [x] Setup: Run `create_tables.py dev` with envvar `CITIBIKE_DRY_RUN=false`
  - [x] Setup: Back up current dbt `profiles.yml` to `profiles.yml.old` then run `python generate_dbt_profile.py` and confirm outputs match
  - [x] Setup: Load seed data
    - [x] NYC holidays: `cd dbt_transformations && dbt seed`
    - [x] Borough boundaries: `cd airflow/dags && python boundaries_pipeline.py`
  - [x] Run trips pipeline (legacy schema): `cd airflow/dags && python trip_pipeline.py 2014 1`
  - [x] Run trips pipline (current schema): `cd airflow/dags && python trip_pipeline.py 2024 2`
  - [x] Run network analysis pipeline: `cd airflow/dags && python network_analysis_pipeline.py`
  - [x] Verify that dotenv_load will print warning output in verbose mode 

2. **Docker containerization**: Create Dockerfile with proper environment setup
3. **Airflow DAG creation**: Wrap pipeline scripts in Airflow operators
4. **K8s deployment**: Set up secrets and ConfigMaps
5. **Cleanup**: Remove or consider removing the client validation output and validation checks, as well as the path resolution, env loading, etc. output

## Files Changed
- `citibike/config/__init__.py` - Environment loading with path resolution
- `citibike/database/bigquery.py` - Simplified client initialization
- `citibike/ingestion/*.py` - Environment variable usage, removed config params
- `citibike/networks/analysis.py` - Environment variable usage
- `airflow/dags/*.py` - Removed config shims, enhanced dry run validation
- `create_tables.py` - Use `load_env_config()`, connection validation
- `generate_dbt_profile.py` - Ready for CI/CD with path resolution

## High-Level Airflow Integration Plan

Based on the current CitiBike pipeline setup, here's the integration approach:

### 1. **Local Airflow Setup with Docker**
- Create `docker-compose.yml` with Airflow services (scheduler, webserver, postgres)
- Mount your entire project directory to give Airflow access to:
  - Your `citibike` utility package
  - `dbt_transformations` directory  
  - Configuration files
  - Existing DAG scripts

### 2. **Convert Existing Scripts to True Airflow DAGs**
Your current scripts in `airflow/dags/` are plain Python scripts with command-line interfaces. Convert them to proper Airflow DAGs using:
- `PythonOperator` for Python extraction scripts
- `BashOperator` for dbt commands (or `dbt-airflow` providers)
- Proper task dependencies and scheduling

### 4. **Local Airflow Setup with Docker**
(After completing config refactor above)
- Install Airflow alongside your existing requirements
- Use the same virtual environment/container so Airflow can import your `citibike` package
- Configure Airflow connections for BigQuery, etc.

### 5. **dbt Integration Strategy**
You're right about running everything in the same container initially. Options:
- **BashOperator** with dbt CLI commands (simplest)
- **dbt-airflow providers** for better integration
- Your existing `run_dbt_command` function works well with `PythonOperator`

### 6. **Deployment Path**
- **Local**: Docker Compose
- **Production**: Kubernetes with Helm charts
- Consider **Google Cloud Composer** (managed Airflow on GCP) since you're using BigQuery

### 7. **Migration Strategy**
1. Keep existing scripts as-is initially
2. Create parallel Airflow DAGs
3. Test thoroughly in local environment
4. Gradually switch from cron/manual to Airflow scheduling

## Implementation Notes

Your approach of having Airflow access everything in the same container is correct for starting out - it avoids complexity of service orchestration while maintaining your existing abstractions.

## Current State Analysis

**Existing DAG Scripts:**
- `airflow/dags/trip_pipeline.py` - Ingests stations/trips, runs dbt dashboard models
- `airflow/dags/network_analysis_pipeline.py` - Network analysis workload
- `airflow/dags/boundaries_pipeline.py` - Boundaries data processing

**Key Dependencies:**
- dbt-core and dbt-bigquery already installed
- `citibike` utility package with config, ingestion, and dbt helpers
- Google Cloud integration for BigQuery