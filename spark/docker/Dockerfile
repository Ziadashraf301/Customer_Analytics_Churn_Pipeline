FROM bitnami/spark:3.5.0

USER root

# Install Python packages required for PySpark scripts
RUN pip install --no-cache-dir pyspark py4j boto3 pandas

# Set Python path to include Spark and py4j
ENV PYTHONPATH=/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.12.1-src.zip

# Keep container running for Airflow exec
CMD ["tail", "-f", "/dev/null"]
