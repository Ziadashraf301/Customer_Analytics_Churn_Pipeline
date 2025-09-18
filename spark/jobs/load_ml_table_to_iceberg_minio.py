from pyspark.sql import SparkSession


# ----------------------------------------------------------------------
# Spark Session
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("CustomerClusteringSim")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.postgresql:postgresql:42.6.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
    .config("spark.driver.memory", "13g")
    # .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()
)

# --------------------------------------------------------------------------
# Read data directly from MinIO S3 Parquet (saved ClickHouse export)
# --------------------------------------------------------------------------
customers_ml = (
    spark.read
    .format("parquet")
    .load("s3a://marketing-data-lake/raw_ml_table/int_customer_profiles_ml")
)

customers_ml.show(truncate=False)
print("Data loaded from MinIO (ClickHouse S3 export).")

# --------------------------------------------------------------------------
# Write to Iceberg (Hadoop catalog)
# --------------------------------------------------------------------------
(
    customers_ml
    .writeTo("minio_catalog.default.int_customer_profiles_ml")
    .createOrReplace()
)

print("Data successfully written to Iceberg (Hadoop catalog): minio_catalog.default.int_customer_profiles_ml")

# --------------------------------------------------------------------------
# Verify read
# --------------------------------------------------------------------------
df_iceberg_read = spark.read.format("iceberg").load("minio_catalog.default.int_customer_profiles_ml")
print(f"Rows in Iceberg table: {df_iceberg_read.count()}")
df_iceberg_read.show(5, truncate=False)

