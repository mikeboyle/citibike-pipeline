# Extend the official Apache Airflow image
FROM apache/airflow:2.10.0

# Switch to root to install system dependencies if needed
USER root

# Install any additional system packages here if needed
# RUN apt-get update && apt-get install -y <packages> && apt-get clean

# Copy and install our Python package and dependencies
COPY requirements.txt /tmp/requirements.txt
COPY . /opt/airflow/citibike-pipeline

# Create initialization script for dbt profile generation
COPY <<EOF /opt/airflow/init-citibike.sh
#!/bin/bash
set -e

echo "🚀 Initializing CitiBike environment..."

# Generate dbt profile if it doesn't exist
if [ ! -f /opt/airflow/dbt_transformations/profiles.yml ]; then
    echo "📝 Generating dbt profile..."
    cd /opt/airflow/citibike-pipeline
    python generate_dbt_profile.py
    echo "✅ dbt profile generated"
else
    echo "✅ dbt profile already exists"
fi

echo "✅ CitiBike initialization complete"
EOF

# Clean up any existing egg-info as root, make script executable, then fix ownership
RUN rm -rf /opt/airflow/citibike-pipeline/citibike.egg-info && \
    chown -R airflow:root /opt/airflow/citibike-pipeline && \
    chmod +x /opt/airflow/init-citibike.sh

# Switch back to airflow user
USER airflow

# Install our citibike package and its dependencies
RUN pip install -r /tmp/requirements.txt && \
    pip install -e /opt/airflow/citibike-pipeline

# Set the initialization script to run before Airflow commands
# This gets executed by the entrypoint before the main command
ENV _AIRFLOW_CONTAINER_INIT_SCRIPT=/opt/airflow/init-citibike.sh