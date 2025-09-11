# Airflow Integration Strategy

## Overview

We've built a simplified Airflow development environment that supports both local development and production deployment using a single Docker image approach with environment-specific configuration.

## Current Architecture

### **Simplified Development Setup**
- **Single Volume Mount**: `- .:/opt/airflow` (entire repo mounted)
- **Flattened Structure**: `dags/` and `plugins/` at repo root (matches Airflow conventions)
- **Runtime Package Installation**: `pip install -e .` happens via init script after volume mount
- **Clean Dockerfile**: Only installs dependencies at build time, everything else at runtime

### **Key Components**
1. **Dockerfile**: Minimal - just installs `requirements.txt` dependencies
2. **init-citibike.sh**: Runtime script that installs package + generates dbt profile
3. **docker-compose.yaml**: Single volume mount with simplified configuration
4. **DAGs**: Located at `/dags/` in repo root for Airflow auto-discovery

### **Why This Architecture**

**Development Benefits:**
- ✅ **Live Editing**: All code changes immediately visible (no rebuilds)
- ✅ **Single Source of Truth**: No dual directories causing path confusion
- ✅ **Fast Iteration**: Change DAGs, dbt models, Python code - all live
- ✅ **Simple Debugging**: What you see in your editor is what runs

**Production Ready:**
- ✅ **Same Image Base**: Dev and prod use same Dockerfile foundation  
- ✅ **Environment Parity**: Same code paths in both environments
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
COPY . /opt/airflow

RUN pip install -r /tmp/requirements.txt && \
    if [ "$ENV" = "prod" ]; then \
        pip install -e . && \
        python generate_dbt_profile.py; \
    fi
```

### **Development vs Production**

| Aspect | Development | Production |
|--------|-------------|------------|
| **Code Source** | Volume mount (live) | Baked into image (immutable) |
| **Package Install** | Runtime via init script | Build time |
| **Profile Generation** | Runtime via init script | Build time |
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
1. **Simplified Docker Architecture** - Single volume mount, no dual directories
2. **Flattened Project Structure** - Airflow-standard layout  
3. **Runtime Package Installation** - Supports live editing
4. **Init Script Strategy** - Clean separation of build vs runtime concerns
5. **Environment Configuration** - Works with existing config system
6. **Profile Generation** - Confirm dbt paths resolve correctly
7. **Automated initialization** - Runs just before the Airflow entrypoint script
8. **DAG Testing** - Verify boundaries_pipeline works end-to-end

### 📋 **Next Steps**
0. **Update README.md** - New instructions to run local development through Airflow UI or CLI only
1. **Complete DAG Conversion** - Convert remaining pipeline scripts
2. **Test dry-run functionality with DAG parameters** (we never fully tested this)
3. **Improve dbt logging in `run_dbt_command()` helper**
  - Better integration with Airflow logs (we fixed basic capture but could be cleaner, right now it still logs to dbt_transformations/logs)
4. **Production Build Args** - Add ENV=prod support to Dockerfile  
5. **Kubernetes Manifests** - Create production deployment configs
6. **CI/CD Pipeline** - Automate build and deploy process

## Architecture Benefits

**Simplicity**: Single volume mount eliminates path confusion
**Flexibility**: Same image works dev to prod with different configs  
**Performance**: Fast development iteration, optimized production builds
**Maintainability**: Standard Airflow structure, minimal custom logic

This approach gives us the best of both worlds: developer-friendly local environment with a clear path to production deployment.