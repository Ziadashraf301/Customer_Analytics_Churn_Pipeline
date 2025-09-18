# TaxiTrack/Dockerfile
FROM apache/airflow:2.10.2

USER root

# Install necessary packages and Docker Compose
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose && \
    chmod +x /usr/local/bin/docker-compose && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# mount the dags directory
COPY dags/requirements.txt /tmp/

# install the requirements
RUN pip install --no-cache-dir -r /tmp/requirements.txt