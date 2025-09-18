from pyspark.sql import SparkSession, functions as F
from datetime import datetime

# -------------------------------
# Spark session
# -------------------------------
spark = (
    SparkSession.builder
    .appName("CustomerClusteringMonitor")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.postgresql:postgresql:42.6.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    # MinIO configs
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg catalog
    .config("spark.sql.catalog.minio_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio_catalog.type", "hadoop")
    .config("spark.sql.catalog.minio_catalog.warehouse", "s3a://marketing-data-lake/iceberg/")
    .config("spark.default.parallelism", "200")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.memory", "12g")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()
)

# -------------------------------
# Read log table
# -------------------------------
log_table = "minio_catalog.default.customer_clusters_log"
df_log = spark.read.format("iceberg").load(log_table)

# -------------------------------
# Extract latest metrics
# -------------------------------
latest_run = df_log.orderBy(F.col("run_date").desc())
metrics_steps = ["scaler_params", "pca_params", "kmeans_metrics"]
metrics_df = latest_run.filter(F.col("step").isin(metrics_steps))

metrics_df.show(100,truncate=False)

# -------------------------------
# Stop Spark
# -------------------------------
spark.stop()
