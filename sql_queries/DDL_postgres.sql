-- Active: 1754904541302@@127.0.0.1@5432@marketing_dw@marts
-- SQL command to create the raw_stream schema (if it doesn't exist)
CREATE SCHEMA IF NOT EXISTS raw_batch;
CREATE SCHEMA IF NOT EXISTS raw_stream;

-- SQL commands to create the tables for Flink to write into
CREATE TABLE IF NOT EXISTS raw_stream.website_events_stream_flink (
    user_id VARCHAR(255) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    page_url VARCHAR(255),
    product_id VARCHAR(255),
    session_id VARCHAR(255) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    search_query VARCHAR(255),
    _processed_at TIMESTAMP NOT NULL
);


CREATE TABLE raw_stream.purchase_events_stream_flink (
    order_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    purchase_time TIMESTAMP(3) NOT NULL,
    total_amount DECIMAL(10, 2),
    product_ids TEXT,
    payment_method VARCHAR(255),
    shipping_address TEXT,
    order_status VARCHAR(255),
    currency VARCHAR(10),
    _processed_at TIMESTAMP(3) NOT NULL
);


CREATE TABLE purchase_events_agg_flink (
    window_start TIMESTAMP(3) NOT NULL,    -- Start of the aggregation window
    window_end TIMESTAMP(3) NOT NULL,      -- End of the aggregation window
    total_orders BIGINT NOT NULL,          -- Number of orders in the window
    total_amount DECIMAL(20,2) NOT NULL,   -- Total purchase amount in the window
    avg_amount DECIMAL(20,2) NOT NULL,     -- Average purchase amount in the window
    total_products BIGINT NOT NULL,        -- Total number of products purchased in the window
    updated_at TIMESTAMP(3) NOT NULL       -- Timestamp when the row was inserted
);

CREATE TABLE website_events_agg_flink (
    window_start TIMESTAMP(3) NOT NULL,       -- Start of the aggregation window
    window_end TIMESTAMP(3) NOT NULL,         -- End of the aggregation window
    total_events BIGINT NOT NULL,             -- Total number of events in the window
    unique_users BIGINT NOT NULL,             -- Number of distinct users in the window
    total_page_views BIGINT NOT NULL,         -- Number of events with page_url
    total_product_clicks BIGINT NOT NULL,     -- Number of events with product_id
    updated_at TIMESTAMP(3) NOT NULL          -- Timestamp when the row was inserted
);