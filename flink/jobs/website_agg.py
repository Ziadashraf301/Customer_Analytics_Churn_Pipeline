import os
import logging
import signal
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from typing import Optional
import argparse


class WebsiteEventsJob:
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
            'topic' = '{self._env_var("KAFKA_WEBSITE_TOPIC")}',
            'properties.bootstrap.servers' = '{self._env_var("KAFKA_BOOTSTRAP_SERVERS")}',
            'properties.group.id' = '{self._env_var("KAFKA_GROUP")}-agg',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
        """)
        self.log.info("Kafka source table created.")

    # --------------------
    # Postgres sink
    # --------------------
    def create_postgres_sink(self):
        self.log.info("Creating PostgreSQL sink table for website aggregates...")
        self.t_env.execute_sql(f"""
        CREATE TABLE {self._env_var("PG_WEBSITE_AGG_TABLE")} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_events BIGINT,
            unique_users BIGINT,
            total_page_views BIGINT,
            total_product_clicks BIGINT,
            updated_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{self._env_var("POSTGRES_URL")}',
            'table-name' = '{self._env_var("PG_SCHEMA")}.{self._env_var("PG_WEBSITE_AGG_TABLE")}',
            'username' = '{self._env_var("POSTGRES_USER")}',
            'password' = '{self._env_var("POSTGRES_PASSWORD")}',
            'sink.buffer-flush.max-rows' = '1',
            'sink.buffer-flush.interval' = '0s',
            'driver' = 'org.postgresql.Driver'
        )
        """)
        self.log.info("PostgreSQL sink table for website aggregates created.")

    # --------------------
    # Run job
    # --------------------
    def run(self):
        self.create_kafka_source()
        self.create_postgres_sink()

        self.log.info("Starting continuous aggregation insert into PostgreSQL...")
        result = self.t_env.execute_sql(f"""
        INSERT INTO {self._env_var("PG_WEBSITE_AGG_TABLE")}
        SELECT
            TUMBLE_START(event_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_timestamp, INTERVAL '1' MINUTE) AS window_end,
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS unique_users,
            SUM(CASE WHEN page_url IS NOT NULL THEN 1 ELSE 0 END) AS total_page_views,
            SUM(CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END) AS total_product_clicks,
            CURRENT_TIMESTAMP AS updated_at
        FROM kafka_source
        GROUP BY TUMBLE(event_timestamp, INTERVAL '1' MINUTE)
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

        self.log.info("Website events aggregation job stopped.")


# --------------------
# Main Entrypoint
# --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=0, help="Job timeout in seconds (0 = no timeout)")
    args = parser.parse_args()

    job = WebsiteEventsJob(timeout_seconds=args.timeout)
    job.run()
