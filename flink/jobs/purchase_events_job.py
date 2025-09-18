import os
import logging
import signal
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from typing import Optional
import argparse

class PurchaseEventsJob:
    def __init__(self, timeout_seconds: Optional[int] = None):
        # Logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        self.log = logging.getLogger(self.__class__.__name__)

        # Stop & timeout
        self.stop_requested = False
        self.timeout_seconds = timeout_seconds

        # Flink environment
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.t_env = TableEnvironment.create(env_settings)

        # Register signals
        signal.signal(signal.SIGINT, self._handle_stop_signal)
        signal.signal(signal.SIGTERM, self._handle_stop_signal)

    # --------------------
    # Signal handler
    # --------------------
    def _handle_stop_signal(self, signum, frame):
        self.stop_requested = True
        self.log.info("Stop signal received. Terminating job...")

    # --------------------
    # Env var reader
    # --------------------
    @staticmethod
    def _env_var(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Environment variable '{name}' is not set")
        return value

    # --------------------
    # Kafka source
    # --------------------
    def create_kafka_source(self):
        self.log.info("Creating Kafka purchase events source table...")
        self.t_env.execute_sql(f"""
        CREATE TABLE kafka_purchase_events (
            order_id STRING,
            user_id STRING,
            purchase_time TIMESTAMP_LTZ(3),
            total_amount DECIMAL(10, 2),
            product_ids ARRAY<STRING>,
            payment_method STRING,
            shipping_address STRING,
            order_status STRING,
            currency STRING,
            _processed_at_flink AS PROCTIME(),
            WATERMARK FOR purchase_time AS purchase_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{self._env_var("KAFKA_PURCHASE_TOPIC")}',
            'properties.bootstrap.servers' = '{self._env_var("KAFKA_BOOTSTRAP_SERVERS")}',
            'properties.group.id' = '{self._env_var("KAFKA_GROUP")}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
        """)
        self.log.info("Kafka purchase events source table created.")

    # --------------------
    # Postgres sink
    # --------------------
    def create_postgres_sink(self):
        self.log.info("Creating PostgreSQL sink table for purchase events...")
        self.t_env.execute_sql(f"""
        CREATE TABLE {self._env_var("PG_PURCHASE_TABLE")} (
            order_id STRING NOT NULL,
            user_id STRING NOT NULL,
            purchase_time TIMESTAMP(3) NOT NULL,
            total_amount DECIMAL(10, 2),
            product_ids STRING,
            payment_method STRING,
            shipping_address STRING,
            order_status STRING,
            currency STRING,
            _processed_at TIMESTAMP(3) NOT NULL
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{self._env_var("POSTGRES_URL")}',
            'table-name' = '{self._env_var("PG_SCHEMA")}.{self._env_var("PG_PURCHASE_TABLE")}',
            'username' = '{self._env_var("POSTGRES_USER")}',
            'password' = '{self._env_var("POSTGRES_PASSWORD")}',
            'driver' = 'org.postgresql.Driver'
        )
        """)
        self.log.info("PostgreSQL sink table for purchase events created.")

    # --------------------
    # Run job
    # --------------------
    def run(self):
        self.create_kafka_source()
        self.create_postgres_sink()

        self.log.info("Starting continuous insert for purchase events...")
        result = self.t_env.execute_sql(f"""
        INSERT INTO {self._env_var("PG_PURCHASE_TABLE")}
        SELECT
            order_id,
            user_id,
            COALESCE(purchase_time, CURRENT_TIMESTAMP) AS purchase_time,
            total_amount,
            CAST(product_ids AS STRING) AS product_ids,
            payment_method,
            shipping_address,
            order_status,
            currency,
            CURRENT_TIMESTAMP AS _processed_at
        FROM kafka_purchase_events
        """)

        # Get job client for control
        job_client = result.get_job_client()
        self.log.info(f"Flink job submitted with JobID={job_client.get_job_id()}")

        # Keep alive with optional timeout
        start_time = time.time()
        while not self.stop_requested:
            if self.timeout_seconds and (time.time() - start_time > self.timeout_seconds):
                self.log.info(f"Timeout of {self.timeout_seconds}s reached. Cancelling Flink job...")
                job_client.cancel()
                self.log.info("Flink job cancelled due to timeout.")
                break
            time.sleep(1)

        self.log.info("Purchase events job stopped.")


# --------------------
# Main Entrypoint
# --------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=0, help="Job timeout in seconds (0 = no timeout)")
    args = parser.parse_args()

    # Example: run with 60s timeout for testing
    job = PurchaseEventsJob(timeout_seconds=args.timeout)
    job.run()
