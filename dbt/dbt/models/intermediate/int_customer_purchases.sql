-- models/intermediate/int_customer_purchases.sql

-- This dbt model aggregates purchase history data solely from streaming sources,
-- providing key metrics like total spend and number of orders based on real-time data.

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        cluster_by=['customer_id'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    p.customer_id,
    COUNT(p.order_id) AS total_orders,
    SUM(p.order_total_amount_usd) AS total_spend_usd,
    AVG(p.order_total_amount_usd) AS avg_order_value_usd,
    MIN(p.order_purchase_timestamp_utc) AS first_purchase_timestamp,
    MAX(p.order_purchase_timestamp_utc) AS last_purchase_timestamp,

    -- Use dateDiff in ClickHouse instead of subtraction
    dateDiff('day', MAX(p.order_purchase_timestamp_utc), now()) AS days_since_last_purchase,

    -- Orders/spend in last 30 days
    SUM(CASE WHEN p.order_purchase_timestamp_utc >= now() - INTERVAL 30 DAY THEN 1 ELSE 0 END) AS orders_last_30_days,
    SUM(CASE WHEN p.order_purchase_timestamp_utc >= now() - INTERVAL 30 DAY THEN p.order_total_amount_usd ELSE 0 END) AS spend_last_30_days_usd,

    -- Order status counts
    SUM(CASE WHEN order_status = 'processing' THEN 1 ELSE 0 END) AS processing_orders,
    SUM(CASE WHEN order_status = 'shipped' THEN 1 ELSE 0 END) AS shipped_orders,
    SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) AS returned_orders,
    SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,

    -- Count number of products (split string by comma inside JSON-like array)
    SUM(
        length(splitByChar(',', replaceAll(replaceAll(order_product_ids_json, '[', ''), ']', '')))
    ) AS num_products,

    now() AS dbt_processed_at

FROM {{ ref('stg_purchase_events') }} p

{% if is_incremental() %}
  -- Only scan new orders since last run
  WHERE p.order_purchase_timestamp_utc > (SELECT max(last_purchase_timestamp) FROM {{ this }})
{% endif %}

GROUP BY p.customer_id
