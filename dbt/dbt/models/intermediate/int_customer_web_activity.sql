-- models/intermediate/int_customer_web_activity.sql

-- This dbt model aggregates website event data by customer,
-- providing key metrics related to their online activity.

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        cluster_by=['customer_id'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    customer_id,
    COUNT(event_id) AS total_web_events,
    COUNT(DISTINCT page_url) AS unique_pages_visited,
    COUNT(session_id) AS sessions,
    MIN(event_timestamp_utc) AS first_web_activity_timestamp,    
    MAX(event_timestamp_utc) AS last_web_activity_timestamp,

    -- Calculate days since last web activity
    dateDiff('day', MAX(event_timestamp_utc), now()) AS days_since_last_web_activity,

    -- Count specific important events
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_add_to_carts,
    SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS total_searches,
    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS total_product_views,

    now() AS dbt_loaded_at

FROM {{ ref('stg_website_events') }}

{% if is_incremental() %}
  -- Only scan new events since last run
  WHERE event_timestamp_utc > (SELECT max(last_web_activity_timestamp) FROM {{ this }})
{% endif %}

GROUP BY customer_id
