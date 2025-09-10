# Airflow Integration Deployment Strategy

## Current Status: Local Airflow Development Environment

### ✅ **Completed Setup:**
1. **Environment Configuration Refactor** - Centralized config loading with `load_env_config()`
2. **Custom Airflow Docker Image** - Extends `apache/airflow:2.10.0` with citibike package
3. **Docker Compose Configuration** - Official Airflow setup with custom extensions
4. **Project Structure Alignment** - Volume mounts match existing directory structure
5. **Centralized Logging** - Single `./logs` directory for all log output
6. **AIRFLOW_UID Strategy** - Shell variable substitution (no separate .env files)

### 🚧 **Current Issue:** 
Docker Engine version 20.10.17 doesn't support heredoc syntax in Dockerfile. Need to either:
- Update Docker to version 24.x+ (recommended)
- Modify Dockerfile for older Docker compatibility

### 📋 **Next Steps:**
1. **Resolve Docker Version** - Update Docker Desktop to get modern Docker Engine
2. **Test Basic Airflow Setup** - Verify `docker-compose up` works
3. **Convert Pipeline Scripts to DAGs** - Transform CLI scripts to proper Airflow DAGs
4. **Test DAG Execution** - Verify pipelines work through Airflow UI

### 🎯 **Current Goal:** 
Get local Airflow development environment running where:
- `AIRFLOW_UID=$(id -u) docker-compose up` starts Airflow
- Access UI at localhost:8080 (airflow/airflow)
- DAGs appear and can be triggered from UI
- Logs centralized in `./logs`

## Environment Strategy Across Deployments

### Local Development (Current Implementation)
```bash
# Direct script execution (still works)
export CITIBIKE_ENV=dev
python airflow/dags/trip_pipeline.py 2024 6

# Airflow development (new approach)
AIRFLOW_UID=$(id -u) docker-compose up
# Access UI at localhost:8080
```

**How it works:**
- `load_env_config('dev')` loads `config/dev.env`
- Credentials file: `config/service-account.json` (relative path resolved to absolute)
- DBT profile: Auto-generated during container startup

### Docker Development (What We Built)
```dockerfile
# Dockerfile
FROM apache/airflow:2.10.0
USER airflow
COPY requirements.txt /tmp/requirements.txt
COPY . /opt/airflow/citibike-pipeline
RUN pip install --user -r /tmp/requirements.txt && \
    pip install --user -e /opt/airflow/citibike-pipeline

# Auto-generate dbt profile on container startup
# (dbt profile generation script created inline)
```

```yaml
# docker-compose.yaml (extends official Airflow setup)
x-airflow-common:
  build: .  # Use our custom image
  environment:
    CITIBIKE_ENV: ${CITIBIKE_ENV:-dev}
    PYTHONPATH: /opt/airflow/citibike-pipeline
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./config:/opt/airflow/config
    - ./airflow/plugins:/opt/airflow/plugins  
    - ./dbt_transformations:/opt/airflow/dbt_transformations
    - ./logs:/opt/airflow/logs
  user: "${AIRFLOW_UID:-50000}:0"  # Shell variable substitution
```

**How it works:**
- Extends official Airflow docker-compose.yaml
- Mounts existing project structure into containers
- AIRFLOW_UID set via shell: `AIRFLOW_UID=$(id -u) docker-compose up`
- Config files mounted, dbt profile generated automatically
- All services (webserver, scheduler, workers) use same custom image

### Future: K8s Production
```yaml
# ConfigMap for non-sensitive config
apiVersion: v1
kind: ConfigMap
metadata:
  name: citibike-config
data:
  GCP_PROJECT_ID: "your-project-prod"
  BQ_DATASET: "citibike"
  GBFS_STATION_URL: "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
  TRIP_DATA_URL: "https://s3.amazonaws.com/tripdata"

---
# Secret for credentials
apiVersion: v1
kind: Secret
metadata:
  name: citibike-credentials
type: Opaque
data:
  service-account.json: <base64-encoded-json-key>

---
# Use same Docker image, different env injection
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
spec:
  template:
    spec:
      containers:
      - name: airflow-scheduler
        image: citibike-pipeline:latest  # Same image we're building now
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
- Same Docker image as local development
- Environment variables injected from ConfigMaps/Secrets
- No config file mounting needed
- DBT profile generated during container startup (same as local)

## Key Design Decisions

### 1. **Single Docker Image Strategy**
- One image works for all Airflow services (webserver, scheduler, workers)
- Contains citibike package + dependencies + dbt profile generation
- Scales from local development to K8s production

### 2. **AIRFLOW_UID Without .env Files**
- Uses shell variable substitution: `${AIRFLOW_UID:-50000}`
- No separate .env files to manage
- Each developer runs: `AIRFLOW_UID=$(id -u) docker-compose up`

### 3. **Project Structure Preservation**
- Volumes mount existing directories (`./airflow/dags`, `./config`, etc.)
- No restructuring of existing codebase required
- Maintains compatibility with direct script execution

### 4. **Environment Configuration Compatibility**
- Local: Uses `config/dev.env` files (existing approach)
- Docker: Mounts config files, same `load_env_config()` approach  
- K8s: Direct environment variable injection

### 5. **Centralized Logging**
- Single `./logs` directory for all log output
- Airflow manages log organization by DAG/task/date
- Works across local and containerized environments

## Migration Strategy

### Phase 1: Local Airflow Development (Current)
- ✅ Custom Docker image with citibike package
- ✅ Official Airflow docker-compose setup extended
- 🚧 Resolve Docker version compatibility
- ⏭️ Convert CLI scripts to proper Airflow DAGs

### Phase 2: DAG Development  
- Convert `trip_pipeline.py` to Airflow DAG
- Convert `network_analysis_pipeline.py` to Airflow DAG
- Convert `boundaries_pipeline.py` to Airflow DAG
- Test scheduling and dependencies

### Phase 3: Production Deployment
- Build and push Docker images to registry
- Create K8s manifests with ConfigMaps/Secrets
- Deploy to production K8s cluster

## Current Blockers

1. **Docker Version**: Need Docker Engine 24.x+ for heredoc syntax in Dockerfile
2. **DAG Conversion**: CLI scripts need conversion to proper Airflow DAG format
3. **Testing**: Need to verify end-to-end pipeline execution through Airflow UI