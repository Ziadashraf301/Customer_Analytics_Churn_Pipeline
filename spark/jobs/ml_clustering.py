import os
import time
from datetime import datetime
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StandardScalerModel, PCA, PCAModel
from pyspark.ml.clustering import KMeans, KMeansModel

logging.basicConfig(level=logging.INFO)

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
    .config("spark.driver.memory", "12g")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()
)

logging.info("✅ Spark session started.")

# ----------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------
table_name = "minio_catalog.default.int_customer_profiles_ml"
id_col = "customer_id"
k = 3

# model directories
scaler_path = "s3a://marketing-data-lake/models/scaler_customer"
pca_path    = "s3a://marketing-data-lake/models/pca_customer"
kmeans_path = "s3a://marketing-data-lake/models/kmeans_customer"

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
    "delivered_orders",
    "returned_orders",
    "canceled_orders",
    "num_products",
    "total_web_events",
    "unique_pages_visited",
    "unique_sessions",
    "days_since_last_web_activity",
    "total_add_to_carts",
    "total_searches",
    "total_page_views",
    "total_product_views",
]

# ----------------------------------------------------------------------
# Logging helper
# ----------------------------------------------------------------------
def log_step(step, status, message):
    df_log = spark.createDataFrame(
        [(datetime.now(), step, status, message)],
        ["run_date", "step", "status", "message"]
    )
    if spark.catalog._jcatalog.tableExists("minio_catalog.default.customer_clusters_log"):
        df_log.writeTo("minio_catalog.default.customer_clusters_log").append()
    else:
        df_log.writeTo("minio_catalog.default.customer_clusters_log").create()

# ----------------------------------------------------------------------
# Model freshness checker
# ----------------------------------------------------------------------
def load_or_train_model(model_class, model_path, train_fn, max_age_days=7):
    """
    Load a model if fresh, otherwise retrain and overwrite.
    """
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(model_path)

        if fs.exists(path):
            # Get last modified timestamp
            file_status = fs.getFileStatus(path)
            last_modified = file_status.getModificationTime() / 1000  # ms → sec
            age_days = (time.time() - last_modified) / (24 * 3600)

            if age_days <= max_age_days:
                log_step("model_load", "INFO", f"Loaded {model_class.__name__} from {model_path} (age={age_days:.1f} days)")
                return model_class.load(model_path)
            else:
                log_step("model_stale", "INFO", f"{model_class.__name__} at {model_path} is stale (age={age_days:.1f} days). Retraining...")
        else:
            log_step("model_missing", "INFO", f"No {model_class.__name__} found at {model_path}, training new one...")

    except Exception as e:
        log_step("model_check", "ERROR", f"Error checking {model_class.__name__}: {str(e)}. Training new one...")

    # --- Train new model ---
    model = train_fn()
    model.write().overwrite().save(model_path)
    log_step("model_train", "INFO", f"Trained and saved new {model_class.__name__} to {model_path}")
    return model

# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------
try:
    log_step("load_data", "INFO", f"Loading table {table_name}")
    df = spark.read.format("iceberg").load(table_name)
except Exception as e:
    log_step("load_data", "ERROR", str(e))
    raise

assert feat_cols, "No numeric feature columns found."
feat_cols = [c for c in feat_cols if c in df.columns]
assert feat_cols, "❌ No numeric feature columns found."

# Cast Decimal -> Double
for f in df.schema.fields:
    if f.name in feat_cols and isinstance(f.dataType, DecimalType):
        df = df.withColumn(f.name, F.col(f.name).cast("double"))

df_num = df.select(id_col, *feat_cols).fillna(0)
df.unpersist()
del df

# ----------------------------------------------------------------------
# Assemble features
# ----------------------------------------------------------------------
log_step("assemble_features", "INFO", "Assembling features")
assembler = VectorAssembler(inputCols=feat_cols, outputCol="features")
df_vec = assembler.transform(df_num)
del df_num

# ----------------------------------------------------------------------
# Standard Scaler
# ----------------------------------------------------------------------
scaler_model = load_or_train_model(
    StandardScalerModel,
    scaler_path,
    lambda: StandardScaler(inputCol="features", outputCol="scaled", withStd=True, withMean=True).fit(df_vec)
)
df_scaled = scaler_model.transform(df_vec)
df_vec.unpersist()

# ----------------------------------------------------------------------
# PCA
# ----------------------------------------------------------------------
pca_model = load_or_train_model(
    PCAModel,
    pca_path,
    lambda: PCA(k=3, inputCol="scaled", outputCol="pca2").fit(df_scaled)
)
df_pca = pca_model.transform(df_scaled)
df_scaled.unpersist()

# ----------------------------------------------------------------------
# KMeans
# ----------------------------------------------------------------------
kmeans_model = load_or_train_model(
    KMeansModel,
    kmeans_path,
    lambda: KMeans(k=k, seed=42, featuresCol="pca2", predictionCol="cluster").fit(df_pca)
)
df_clusters = kmeans_model.transform(df_pca).select(
    id_col,
    "cluster"
).withColumn("run_date", F.current_timestamp())
df_pca.unpersist()

# ----------------------------------------------------------------------
# Save to Iceberg
# ----------------------------------------------------------------------
try:
    log_step("save_clusters", "INFO", "Writing clusters to Iceberg table `customer_clusters`")
    df_clusters.writeTo("minio_catalog.default.customer_clusters").createOrReplace()
    log_step("save_clusters", "SUCCESS", "Clusters saved successfully")
except Exception as e:
    log_step("save_clusters", "ERROR", str(e))
    raise

df_clusters.show(5, truncate=False)
df_clusters.unpersist()

log_step("process_complete", "SUCCESS", "Customer clustering completed successfully")
