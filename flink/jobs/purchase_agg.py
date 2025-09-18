import os
import logging
import signal
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from typing import Optional
import argparse


class PurchaseEventsAggJob:
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

        self.job_client = None

    # --------------------
    # Signal handler
    # --------------------
    def _handle_stop_signal(self, signum, frame):
        self.stop_requested = True
        self.log.info("Stop signal received. Terminating job...")
        self._cancel_job()

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
        self.log.info("Creating PostgreSQL sink table for aggregates...")
        self.t_env.execute_sql(f"""
        CREATE TABLE {self._env_var("PG_PURCHASE_AGG_TABLE")} (
            window_start TIMESTAMP(3) NOT NULL,
            window_end TIMESTAMP(3) NOT NULL,
            total_orders BIGINT NOT NULL,
            total_amount DECIMAL(20,2),
            avg_amount DECIMAL(20,2),
            total_products BIGINT,
            updated_at TIMESTAMP(3) NOT NULL
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{self._env_var("POSTGRES_URL")}',
            'table-name' = '{self._env_var("PG_SCHEMA")}.{self._env_var("PG_PURCHASE_AGG_TABLE")}',
            'username' = '{self._env_var("POSTGRES_USER")}',
            'password' = '{self._env_var("POSTGRES_PASSWORD")}',
            'sink.buffer-flush.max-rows' = '1',
            'sink.buffer-flush.interval' = '0s',
            'driver' = 'org.postgresql.Driver'
        )
        """)
        self.log.info("PostgreSQL sink table for aggregates created.")

    # --------------------
    # Cancel helper
    # --------------------
    def _cancel_job(self):
        if self.job_client:
            try:
                self.job_client.cancel()
                self.log.info("Flink job cancelled.")
            except Exception as e:
                self.log.warning(f"Failed to cancel Flink job: {e}")

    # --------------------
    # Run job
    # --------------------
    def run(self):
        self.create_kafka_source()
        self.create_postgres_sink()

        self.log.info("Starting continuous aggregation insert into PostgreSQL...")
        result = self.t_env.execute_sql(f"""
        INSERT INTO {self._env_var("PG_PURCHASE_AGG_TABLE")}
        SELECT
            TUMBLE_START(purchase_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(purchase_time, INTERVAL '1' MINUTE) AS window_end,
            COUNT(*) AS total_orders,
            SUM(total_amount) AS total_amount,
            AVG(total_amount) AS avg_amount,
            SUM(CARDINALITY(product_ids)) AS total_products,
            CURRENT_TIMESTAMP AS updated_at
        FROM kafka_purchase_events
        GROUP BY TUMBLE(purchase_time, INTERVAL '1' MINUTE)
        """)

        # Get job client for control
        self.job_client = result.get_job_client()
        self.log.info(f"Flink job submitted with JobID={self.job_client.get_job_id()}")

        # Keep alive with optional timeout
        start_time = time.time()
        try:
            while not self.stop_requested:
                if self.timeout_seconds and (time.time() - start_time > self.timeout_seconds):
                    self.log.info(f"Timeout of {self.timeout_seconds}s reached. Cancelling Flink job...")
                    self._cancel_job()
                    break
                time.sleep(1)
        finally:
            self.log.info("Purchase events aggregation job stopped.")


# --------------------
# Main Entrypoint
# --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=0, help="Job timeout in seconds (0 = no timeout)")
    args = parser.parse_args()

    job = PurchaseEventsAggJob(timeout_seconds=args.timeout)
    job.run()
