{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        cluster_by=['customer_id'],
        incremental_strategy='delete+insert'
    )
}}

WITH web_activity AS (
    SELECT * FROM {{ ref('int_customer_web_activity') }}
),
purchases AS (
    SELECT * FROM {{ ref('int_customer_purchases') }}
)

SELECT
    COALESCE(w.customer_id, p.customer_id) AS customer_id,

    -- Web Activity Metrics
    COALESCE(w.total_web_events, 0) AS total_web_events,
    COALESCE(w.unique_pages_visited, 0) AS unique_pages_visited,
    COALESCE(w.sessions, 0) AS sessions,
    COALESCE(w.first_web_activity_timestamp, toDateTime('1900-01-01 00:00:00')) AS first_web_activity_timestamp,
    COALESCE(w.last_web_activity_timestamp, toDateTime('1900-01-01 00:00:00'))  AS last_web_activity_timestamp,
    w.days_since_last_web_activity,
    COALESCE(w.total_page_views, 0) AS total_page_views,
    COALESCE(w.total_add_to_carts, 0) AS total_add_to_carts,
    COALESCE(w.total_searches, 0) AS total_searches,
    COALESCE(w.total_product_views, 0) AS total_product_views,

    -- Purchase Metrics
    COALESCE(p.total_orders, 0) AS total_orders,
    COALESCE(p.total_spend_usd, 0) AS total_spend_usd,
    COALESCE(p.avg_order_value_usd, 0) AS avg_order_value_usd,
    COALESCE(p.first_purchase_timestamp, toDateTime('1900-01-01 00:00:00')) AS first_purchase_timestamp,
    COALESCE(p.last_purchase_timestamp, toDateTime('1900-01-01 00:00:00'))  AS last_purchase_timestamp,
    p.days_since_last_purchase,
    COALESCE(p.orders_last_30_days, 0) AS orders_last_30_days,
    COALESCE(p.spend_last_30_days_usd, 0) AS spend_last_30_days_usd,
    COALESCE(p.processing_orders, 0) AS processing_orders,
    COALESCE(p.shipped_orders, 0) AS shipped_orders,
    COALESCE(p.cancelled_orders, 0) AS cancelled_orders,
    COALESCE(p.returned_orders, 0) AS returned_orders,
    COALESCE(p.completed_orders, 0) AS completed_orders,
    COALESCE(p.num_products, 0) AS num_products,

    now() AS dbt_loaded_at

FROM web_activity w
FULL OUTER JOIN purchases p
    ON w.customer_id = p.customer_id
    
{% if is_incremental() %}
WHERE greatest(
    COALESCE(w.last_web_activity_timestamp, toDateTime('1900-01-01 00:00:00')),
    COALESCE(p.last_purchase_timestamp, toDateTime('1900-01-01 00:00:00'))
) > (
    SELECT greatest(
        COALESCE(max(last_web_activity_timestamp), toDateTime('1900-01-01 00:00:00')),
        COALESCE(max(last_purchase_timestamp), toDateTime('1900-01-01 00:00:00'))
    )
    FROM {{ this }}
)
{% endif %}

SETTINGS join_use_nulls = 1
