{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='delete+insert',
    engine='MergeTree()',
    order_by=['customer_id']
) }}

WITH customers AS (
    SELECT
        customer_id,
        customer_registration_date,
        customer_subscription_tier,
        customer_acquisition_channel,
        customer_email,
        customer_country,
        clickhouse_loaded_at
    FROM {{ source('default', 'stg_customers')
 }}
),
marketing_funnel AS (
    SELECT
        customer_id,
        total_web_events,
        unique_pages_visited,
        sessions,
        first_web_activity_timestamp,
        last_web_activity_timestamp,
        days_since_last_web_activity,
        total_page_views,
        total_add_to_carts,
        total_searches,
        total_product_views,
        total_orders,
        total_spend_usd,
        avg_order_value_usd,
        first_purchase_timestamp,
        last_purchase_timestamp,
        days_since_last_purchase,
        orders_last_30_days,
        spend_last_30_days_usd,
        processing_orders,
        shipped_orders,
        cancelled_orders,
        returned_orders,
        completed_orders,
        num_products
    FROM {{ ref('marketing_funnel') }}
)

SELECT
    c.customer_id,
    c.customer_registration_date,
    c.customer_subscription_tier,
    c.customer_acquisition_channel,
    c.customer_email,
    c.customer_country,
    dateDiff('day', c.customer_registration_date, today()) AS days_since_registration,
    m.total_web_events,
    m.unique_pages_visited,
    m.sessions,
    m.first_web_activity_timestamp,
    m.last_web_activity_timestamp,
    m.days_since_last_web_activity,
    m.total_page_views,
    m.total_add_to_carts,
    m.total_searches,
    m.total_product_views,
    m.total_orders,
    m.total_spend_usd,
    m.avg_order_value_usd,
    m.first_purchase_timestamp,
    m.last_purchase_timestamp,
    m.days_since_last_purchase,
    m.orders_last_30_days,
    m.spend_last_30_days_usd,
    m.processing_orders,
    m.shipped_orders,
    m.cancelled_orders,
    m.returned_orders,
    m.completed_orders,
    m.num_products,
    c.clickhouse_loaded_at,
    now() AS clickhouse_processed_at
FROM customers c
LEFT JOIN marketing_funnel m
    ON c.customer_id = m.customer_id
{% if is_incremental() %}
WHERE greatest(
    COALESCE(m.last_web_activity_timestamp, toDateTime('1900-01-01 00:00:00')),
    COALESCE(m.last_purchase_timestamp, toDateTime('1900-01-01 00:00:00'))
) > (
    SELECT greatest(
        COALESCE(max(last_web_activity_timestamp), toDateTime('1900-01-01 00:00:00')),
        COALESCE(max(last_purchase_timestamp), toDateTime('1900-01-01 00:00:00'))
    )
    FROM {{ this }}
)
{% endif %}