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
    total_amount DECIMAL(20, 2),
    product_ids TEXT,
    payment_method VARCHAR(255),
    shipping_address TEXT,
    order_status VARCHAR(255),
    currency VARCHAR(10),
    _processed_at TIMESTAMP(3) NOT NULL
);


CREATE TABLE raw_stream.purchase_events_agg_flink (
    window_start TIMESTAMP(3) NOT NULL,    
    window_end TIMESTAMP(3) NOT NULL,      
    total_orders BIGINT NOT NULL,          
    total_amount DECIMAL(20,2) NOT NULL,   
    avg_amount DECIMAL(20,2) NOT NULL,     
    total_products BIGINT NOT NULL,        
    updated_at TIMESTAMP(3) NOT NULL       
);

CREATE TABLE raw_stream.website_events_agg_flink (
    window_start TIMESTAMP(3) NOT NULL,       
    window_end TIMESTAMP(3) NOT NULL,         
    total_events BIGINT NOT NULL,             
    unique_users BIGINT NOT NULL,             
    total_page_views BIGINT NOT NULL,         
    total_product_clicks BIGINT NOT NULL,     
    updated_at TIMESTAMP(3) NOT NULL          
);