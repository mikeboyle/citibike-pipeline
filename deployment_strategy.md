# Airflow Integration Strategy

## Overview

We've built a simplified Airflow development environment that supports both local development and production deployment using a selective volume mount approach with environment-specific configuration.

## Current Architecture

### **Selective Volume Mount Development Setup**
- **Selective Volume Mounts**: Only necessary project directories are mounted to container
- **Clean Host Directory**: Airflow-generated files (`airflow.cfg`, `webserver_config.py`) stay in container
- **Flattened Structure**: `dags/` and `plugins/` at repo root (matches Airflow conventions)
- **Runtime Package Installation**: `pip install -e .` happens on every container start for fresh development
- **Clean Dockerfile**: Only installs dependencies at build time, everything else at runtime

### **Key Components**
1. **Dockerfile**: Minimal - installs `requirements.txt` dependencies + embedded init script
2. **Custom Entrypoint**: Runtime script that installs package + generates dbt profile on every startup
3. **docker-compose.yaml**: Selective volume mounts for development directories only
4. **DAGs**: Located at `/dags/` in repo root for Airflow auto-discovery
5. **Scripts Directory**: Container-execution scripts stored in `/scripts/` directory

### **Volume Mount Strategy**
```yaml
volumes:
  # Core project directories
  - ./citibike:/opt/airflow/citibike
  - ./config:/opt/airflow/config
  - ./dags:/opt/airflow/dags
  - ./dbt_transformations:/opt/airflow/dbt_transformations
  - ./plugins:/opt/airflow/plugins
  - ./logs:/opt/airflow/logs
  # Scripts directory
  - ./scripts:/opt/airflow/scripts
  # Required files for package installation
  - ./setup.py:/opt/airflow/setup.py
  - ./requirements.txt:/opt/airflow/requirements.txt
```

### **Why This Architecture**

**Development Benefits:**
- ✅ **Live Editing**: All code changes immediately visible (no rebuilds)
- ✅ **Clean Host Directory**: No Airflow-generated config files cluttering project root
- ✅ **Fast Iteration**: Change DAGs, dbt models, Python code - all live
- ✅ **Simple Debugging**: What you see in your editor is what runs
- ✅ **Fresh Package State**: `pip install -e .` on every startup ensures consistent development environment

**Production Ready:**
- ✅ **Same Image Base**: Dev and prod use same Dockerfile foundation
- ✅ **Environment Parity**: Same code paths in both environments
- ✅ **Explicit Dependencies**: Production builds only include necessary directories
- ✅ **Smaller Images**: `.dockerignore` prevents accidental inclusion of dev artifacts
- ✅ **Scalable**: Ready for container orchestration

## Development Workflow

### **Start Development Environment**
```bash
AIRFLOW_UID=$(id -u) docker compose up
```

### **Access Airflow**
- **UI**: http://localhost:8080 (airflow/airflow)
- **DAGs**: Auto-loaded from `/dags/` directory
- **Logs**: Centralized in `./logs/`

### **Make Changes**
- **DAG Code**: Edit files in `dags/` - changes appear immediately
- **Python Package**: Edit `citibike/` code - changes appear immediately  
- **dbt Models**: Edit `dbt_transformations/` - changes appear immediately
- **Dependencies**: Edit `requirements.txt` - requires rebuild

## Production Deployment Strategy

### **Build Strategy**
```dockerfile
# Production build (future)
FROM apache/airflow:2.10.0
ARG ENV=prod

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Selective copy - only production-needed directories
COPY citibike/ /opt/airflow/citibike/
COPY config/ /opt/airflow/config/
COPY dags/ /opt/airflow/dags/
COPY dbt_transformations/ /opt/airflow/dbt_transformations/
COPY plugins/ /opt/airflow/plugins/
COPY scripts/ /opt/airflow/scripts/
COPY setup.py /opt/airflow/

RUN if [ "$ENV" = "prod" ]; then \
        pip install -e . && \
        python scripts/generate_dbt_profile.py; \
    fi
```

### **Development vs Production**

| Aspect | Development | Production |
|--------|-------------|------------|
| **Code Source** | Selective volume mounts (live) | Selective COPY into image (immutable) |
| **Package Install** | Runtime on every startup | Build time |
| **Profile Generation** | Runtime via scripts/ | Build time via scripts/ |
| **Airflow Config** | Container-only (clean host) | Container-only |
| **Orchestration** | docker-compose | Kubernetes |
| **Scaling** | Single node | Multi-node cluster |

### **Kubernetes Deployment (Future)**
```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: airflow-scheduler
        image: citibike-pipeline:prod
        env:
        - name: CITIBIKE_ENV
          value: "prod"
        # No volumes - self-contained image
```

## Current Status

### ✅ **Completed**
1. **Selective Volume Mount Architecture** - Clean host directory, explicit container mounts
2. **Flattened Project Structure** - Airflow-standard layout
3. **Runtime Package Installation** - Fresh installs on every startup for consistent dev environment
4. **Custom Entrypoint Strategy** - Embedded init script with clean separation of concerns
5. **Environment Configuration** - Works with existing config system
6. **Profile Generation** - Confirm dbt paths resolve correctly
7. **Scripts Directory** - Container-execution scripts organized in dedicated directory
8. **DAG Testing** - Verify boundaries_pipeline works end-to-end
9. **Production Build Strategy** - Selective COPY approach ready for clean production images

### 📋 **Next Steps**
0. **Implement Selective Volume Mounts** - Apply changes from `todos.md` to complete the transition
1. **Update README.md** - New instructions reflecting selective volume mount approach
2. **Complete DAG Conversion** - Convert remaining pipeline scripts
3. **Test dry-run functionality with DAG parameters** (we never fully tested this)
4. **Improve dbt logging in `run_dbt_command()` helper**
   - Better integration with Airflow logs (we fixed basic capture but could be cleaner, right now it still logs to dbt_transformations/logs)
5. **Production Build Args** - Add ENV=prod support to Dockerfile
6. **Kubernetes Manifests** - Create production deployment configs
7. **CI/CD Pipeline** - Automate build and deploy process

## Architecture Benefits

**Simplicity**: Selective volume mounts provide clear boundaries between dev and container environments
**Cleanliness**: No Airflow-generated files cluttering the host project directory
**Flexibility**: Same image base works dev to prod with selective inclusion strategies
**Performance**: Fast development iteration, optimized production builds, fresh package state
**Security**: Explicit control over what gets included in production images
**Maintainability**: Standard Airflow structure, organized script directory, minimal custom logic

This approach gives us the best of both worlds: a clean, developer-friendly local environment with an explicit, secure path to production deployment.