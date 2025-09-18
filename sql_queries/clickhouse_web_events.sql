-- Active: 1756648855428@@127.0.0.1@8123@default
-- Active: 1756648855428@@127.0.0.1@8123@default
CREATE DATABASE IF NOT EXISTS raw_stream;

USE raw_stream;

-- web events table
-- 1) Raw Kafka ingestion table
CREATE TABLE raw_stream.kafka__website_events
(
    user_id String,
    event_timestamp UInt64,    -- epoch micros from source
    event_type String,
    page_url String,
    product_id String,
    session_id String,
    device_type String,
    search_query String,
    _processed_at UInt64 -- epoch micros from Flink
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'marketing_dw.raw_stream.website_events_stream_flink',
         kafka_group_name = 'dev',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url='http://schema-registry:8081';

-- 2) Final storage table (MergeTree)
CREATE TABLE raw_stream.website_events_stream_flink
(
    user_id String,
    event_timestamp DateTime,
    event_type String,
    page_url String,
    product_id String,
    session_id String,
    device_type String,
    search_query String,
    _processed_at DateTime,
    kafka_time DateTime,
    kafka_offset UInt64
) ENGINE = MergeTree
ORDER BY (user_id, event_timestamp)
SETTINGS index_granularity = 8192;

-- 3) Materialized view to transform Kafka → MergeTree
CREATE MATERIALIZED VIEW raw_stream.consumer__website_events
TO raw_stream.website_events_stream_flink
AS
SELECT
    user_id,
    toDateTime(event_timestamp / 1000000) AS event_timestamp,
    event_type,
    page_url,
    product_id,
    session_id,
    device_type,
    search_query,
    toDateTime(_processed_at / 1000000) AS _processed_at,
    _timestamp AS kafka_time,
    _offset AS kafka_offset
FROM raw_stream.kafka__website_events;











