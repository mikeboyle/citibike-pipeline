FROM apache/airflow:2.10.0

# Copy only requirements for build-time dependency installation
COPY requirements.txt /tmp/requirements.txt

# Install dependencies
USER airflow
RUN pip install -r /tmp/requirements.txt

# Create custom entrypoint with embedded init logic (outside volume mount path)
USER root
COPY <<EOF /usr/local/bin/custom-entrypoint.sh
#!/bin/bash
set -e

# Change to the volume-mounted directory
cd /opt/airflow

echo "🚀 Installing citibike package from volume mount..."
pip install -e .

echo "✅ CitiBike initialization complete"

# Call original Airflow entrypoint
exec /entrypoint "\$@"
EOF

RUN chmod +x /usr/local/bin/custom-entrypoint.sh
USER airflow

# Use our custom entrypoint
ENTRYPOINT ["/usr/local/bin/custom-entrypoint.sh"]