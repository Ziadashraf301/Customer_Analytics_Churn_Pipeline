import os
import logging
import signal
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from typing import Optional
import argparse

class WebsiteEventsJob:
    def __init__(self, timeout_seconds: Optional[int] = None):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        self.log = logging.getLogger(self.__class__.__name__)

        # Timeout / graceful stop
        self.stop_requested = False
        self.timeout_seconds = timeout_seconds

        signal.signal(signal.SIGINT, self._handle_stop_signal)
        signal.signal(signal.SIGTERM, self._handle_stop_signal)

        # Flink TableEnvironment
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.t_env = TableEnvironment.create(env_settings)

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
    def env_var(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Environment variable '{name}' is not set")
        return value

    # --------------------
    # Define Kafka source
    # --------------------
    def create_kafka_source(self):
        self.log.info("Creating Kafka source table for website events...")
        self.t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            user_id STRING,
            event_timestamp TIMESTAMP_LTZ(3),  
            event_type STRING,
            page_url STRING,
            product_id STRING,
            session_id STRING,
            device_type STRING,
            search_query STRING,
            _processed_at AS PROCTIME(),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{self.env_var("KAFKA_WEBSITE_TOPIC")}',
            'properties.bootstrap.servers' = '{self.env_var("KAFKA_BOOTSTRAP_SERVERS")}',
            'properties.group.id' = '{self.env_var("KAFKA_GROUP")}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
        """)
        self.log.info("Kafka website events source table created.")

    # --------------------
    # Define Postgres sink
    # --------------------
    def create_postgres_sink(self):
        self.log.info("Creating PostgreSQL sink table for website events...")
        self.t_env.execute_sql(f"""
        CREATE TABLE {self.env_var("PG_WEBSITE_TABLE")} (
            user_id STRING NOT NULL,
            event_timestamp TIMESTAMP(3),
            event_type STRING NOT NULL,
            page_url STRING,
            product_id STRING,
            session_id STRING NOT NULL,
            device_type STRING NOT NULL,
            search_query STRING,
            _processed_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{self.env_var("POSTGRES_URL")}',
            'table-name' = '{self.env_var("PG_SCHEMA")}.{self.env_var("PG_WEBSITE_TABLE")}',
            'username' = '{self.env_var("POSTGRES_USER")}',
            'password' = '{self.env_var("POSTGRES_PASSWORD")}',
            'driver' = 'org.postgresql.Driver'
        )
        """)
        self.log.info("PostgreSQL sink table for website events created.")

    # --------------------
    # Run job
    # --------------------
    def run(self):
        self.create_kafka_source()
        self.create_postgres_sink()

        self.log.info("Starting continuous insert for website events...")
        result = self.t_env.execute_sql(f"""
        INSERT INTO {self.env_var("PG_WEBSITE_TABLE")}
        SELECT
            user_id,
            event_timestamp,
            event_type,
            page_url,
            product_id,
            session_id,
            device_type,
            search_query,
            _processed_at
        FROM kafka_source
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

    job = WebsiteEventsJob(timeout_seconds=args.timeout)
    job.run()