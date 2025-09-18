import os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StandardScalerModel, PCA, PCAModel
from pyspark.ml.clustering import KMeans, KMeansModel
import logging

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
# Helper function to log steps
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
try:
    log_step("scaler", "INFO", f"Attempting to load scaler from {scaler_path}")
    scaler_model = StandardScalerModel.load(scaler_path)
    log_step("scaler", "INFO", f"Loaded existing scaler from {scaler_path}")
except Exception as e:
    log_step("scaler", "INFO", "Training new scaler")
    scaler = StandardScaler(inputCol="features", outputCol="scaled", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_vec)
    scaler_model.save(scaler_path)
    log_step("scaler", "INFO", f"Scaler saved to {scaler_path}")

df_scaled = scaler_model.transform(df_vec)

# Log Scaler parameters
try:
    # Get column names from the assembler
    feature_cols = assembler.getInputCols()

    # Extract scaler parameters
    mean_vals = scaler_model.mean.toArray().tolist()
    std_vals = scaler_model.std.toArray().tolist()

    # Combine into a dict: {col: {"mean": ..., "std": ...}}
    scaler_params = {
        col: {"mean": mean, "std": std}
        for col, mean, std in zip(feature_cols, mean_vals, std_vals)
    }

    log_step("scaler_params", "INFO", f"Scaler parameters: {scaler_params}")

except Exception as e:
    log_step("scaler_params", "ERROR", str(e))

df_vec.unpersist()
del df_vec
# PCA
# ----------------------------------------------------------------------
try:
    log_step("pca", "INFO", f"Attempting to load PCA model from {pca_path}")
    pca_model = PCAModel.load(pca_path)
    log_step("pca", "INFO", f"Loaded existing PCA from {pca_path}")
except Exception as e:
    log_step("pca", "INFO", f"PCA model not found at {pca_path}, training new PCA. Error: {str(e)}")
    pca = PCA(k=3, inputCol="scaled", outputCol="pca2")
    pca_model = pca.fit(df_scaled)
    pca_model.save(pca_path)
    log_step("pca", "INFO", f"PCA saved to {pca_path}")

df_pca = pca_model.transform(df_scaled)
df_pca = pca_model.transform(df_scaled)

# Log PCA explained variance
try:
    explained_var = pca_model.explainedVariance.toArray().tolist()
    log_step("pca_params", "INFO", f"PCA explained variance: {explained_var}")
except Exception as e:
    log_step("pca_params", "ERROR", str(e))

df_scaled.unpersist()
del df_scaled
# KMeans
# ----------------------------------------------------------------------
try:
    log_step("kmeans", "INFO", f"Attempting to load KMeans from {kmeans_path}")
    model = KMeansModel.load(kmeans_path)
    log_step("kmeans", "INFO", f"Loaded existing KMeans from {kmeans_path}")
except Exception as e:
    log_step("kmeans", "INFO", "Training new KMeans on PCA features")
    model = KMeans(k=k, seed=42, featuresCol="pca2", predictionCol="cluster").fit(df_pca)
    model.save(kmeans_path)
    log_step("kmeans", "INFO", f"KMeans model saved to {kmeans_path}")
    log_step("kmeans", "INFO", f"KMeans model saved to {kmeans_path}")

# Log KMeans metrics
try:
    inertia = model.summary.trainingCost if hasattr(model, "summary") else None
    cluster_sizes = model.summary.clusterSizes if hasattr(model, "summary") else None
    log_step("kmeans_metrics", "INFO", f"Inertia: {inertia}, Cluster sizes: {cluster_sizes}")
except Exception as e:
    log_step("kmeans_metrics", "ERROR", str(e))

# ----------------------------------------------------------------------
# Assign clusters
# ----------------------------------------------------------------------
df_clusters = model.transform(df_pca).select(
    id_col,
    "cluster"
).withColumn("run_date", F.current_timestamp())

del df_pca
# ----------------------------------------------------------------------
# Save to Iceberg table `customer_clusters`
# ----------------------------------------------------------------------
try:
    log_step("save_clusters", "INFO", "Writing clusters to Iceberg table `customer_clusters`")
    df_clusters.writeTo("minio_catalog.default.customer_clusters").createOrReplace()
    log_step("save_clusters", "SUCCESS", "Table `customer_clusters` created successfully")
except Exception as e:
    log_step("save_clusters", "ERROR", str(e))
    raise

df_clusters.show(5, truncate=False)

del df_clusters

log_step("process_complete", "SUCCESS", "Customer clustering completed successfully")
