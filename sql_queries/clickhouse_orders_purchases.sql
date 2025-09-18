-- Active: 1756648855428@@127.0.0.1@8123@default
-- Active: 1756648855428@@127.0.0.1@8123@default
CREATE DATABASE IF NOT EXISTS raw_stream;
CREATE DATABASE IF NOT EXISTS marts;

USE raw_stream;

-- web events table
-- 1) Raw Kafka ingestion table
CREATE TABLE raw_stream.kafka__purchase_events
(
    order_id String,
    user_id String,
    purchase_time UInt64,    -- epoch micros from source
    total_amount Decimal(10, 2),
    product_ids String,
    payment_method String,
    shipping_address String,
    order_status String,
    currency String,
    _processed_at UInt64 -- epoch micros from Flink
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'marketing_dw.raw_stream.purchase_events_stream_flink',
         kafka_group_name = 'dev1',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url='http://schema-registry:8081';

-- 2) Final storage table (MergeTree)
CREATE TABLE raw_stream.purchase_events_stream_flink
(
    order_id String,
    user_id String,
    purchase_time DateTime64(3),   -- ✅ match Postgres TIMESTAMP(3)
    total_amount Decimal(10, 2),
    product_ids String,
    payment_method String,
    shipping_address String,
    order_status String,
    currency String,
    _processed_at DateTime64(3),   -- ✅ match Postgres TIMESTAMP(3)
    kafka_time DateTime,
    kafka_offset UInt64
) ENGINE = MergeTree
ORDER BY (order_id)
SETTINGS index_granularity = 8192;



-- 3) Materialized view to transform Kafka → MergeTree
CREATE MATERIALIZED VIEW raw_stream.consumer__purchase_events
TO raw_stream.purchase_events_stream_flink
AS
SELECT
    order_id,
    user_id,
    toDateTime64(purchase_time / 1000, 3) AS purchase_time,  -- ✅ micros → ms
    total_amount,
    product_ids,
    payment_method,
    shipping_address,
    order_status,
    currency,
    toDateTime64(_processed_at / 1000, 3) AS _processed_at, -- ✅ micros → ms
    _timestamp AS kafka_time,
    _offset AS kafka_offset
FROM raw_stream.kafka__purchase_events;











