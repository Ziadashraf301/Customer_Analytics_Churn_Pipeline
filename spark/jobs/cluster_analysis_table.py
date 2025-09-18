import logging
from pyspark.sql import SparkSession, functions as F

# --------------------------
# Setup logging
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ClusterAnalysis")

# --------------------------
# Spark session
# --------------------------
spark = (
    SparkSession.builder
    .appName("CustomerClusteringSim")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.postgresql:postgresql:42.6.0,"
            "com.clickhouse:clickhouse-jdbc:0.8.3,"
            "com.clickhouse.spark:clickhouse-spark-runtime-3.4_2.12:0.8.0,"
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
    # ClickHouse catalog
    .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    .config("spark.sql.catalog.clickhouse.host", "localhost")
    .config("spark.sql.catalog.clickhouse.protocol", "http")
    .config("spark.sql.catalog.clickhouse.http_port", "8123")
    .config("spark.sql.catalog.clickhouse.user", "default")
    .config("spark.sql.catalog.clickhouse.password", "")
    .config("spark.sql.catalog.clickhouse.database", "marts")
    # Performance
    .config("spark.default.parallelism", "200")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.memory", "13g")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()
)

# --------------------------
# Load tables
# --------------------------
logger.info("Loading customer features table ...")
df_customers = spark.table("minio_catalog.default.int_customer_profiles_ml")

logger.info("Loading customer clusters table ...")
df_clusters = spark.table("minio_catalog.default.customer_clusters")

# --------------------------
# Join tables
# --------------------------
logger.info("Joining features with clusters ...")
df_joined = df_customers.join(df_clusters, on="customer_id", how="inner")

# --------------------------
# Define feature columns
# --------------------------
feat_cols = [
    "days_since_registration",
    "total_orders",
    "total_spend_usd",
    "avg_order_value_usd",
    "days_since_last_purchase",
    "orders_last_30_days",
    "spend_last_30_days_usd",
    "processing_orders",
    "shipped_orders",
    "completed_orders",
    "returned_orders",
    "cancelled_orders",
    "num_products",
    "total_web_events",
    "unique_pages_visited",
    "days_since_last_web_activity",
    "total_add_to_carts",
    "total_searches",
    "total_page_views",
    "total_product_views",
]

# --------------------------
# Cluster-level aggregation
# --------------------------
logger.info("Aggregating cluster statistics ...")

agg_exprs = [F.avg(c).alias(f"avg_{c}") for c in feat_cols]
agg_exprs.append(F.count("*").alias("rows_in_cluster"))

df_cluster_analysis = (
    df_joined.groupBy("cluster")
    .agg(*agg_exprs)
    .orderBy("cluster")
    .withColumn("analysis_timestamp", F.current_timestamp())
)

logger.info("Cluster analysis complete ✅")

# --------------------------
# Write results to ClickHouse
# --------------------------
logger.info("Writing DataFrame to ClickHouse table marts.customers_clusters_analysis ...")

(
    df_cluster_analysis.write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/marts")
    .option("dbtable", "customers_clusters_analysis")
    .option("user", "default")
    .option("password", "")
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .mode("append")
    .save()
)

logger.info("Upload to ClickHouse complete 🚀")
