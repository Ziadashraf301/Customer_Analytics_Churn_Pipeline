CREATE TABLE marts.customers_clusters_analysis
(
    cluster Int32,
    avg_days_since_registration Float64,
    avg_total_orders Float64,
    avg_total_spend_usd Float64,
    avg_avg_order_value_usd Float64,
    avg_days_since_last_purchase Float64,
    avg_orders_last_30_days Float64,
    avg_spend_last_30_days_usd Float64,
    avg_processing_orders Float64,
    avg_shipped_orders Float64,
    avg_completed_orders Float64,
    avg_returned_orders Float64,
    avg_cancelled_orders Float64,
    avg_num_products Float64,
    avg_total_web_events Float64,
    avg_unique_pages_visited Float64,
    avg_days_since_last_web_activity Float64,
    avg_total_add_to_carts Float64,
    avg_total_searches Float64,
    avg_total_page_views Float64,
    avg_total_product_views Float64,
    rows_in_cluster UInt64,
    analysis_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (cluster, analysis_timestamp);
