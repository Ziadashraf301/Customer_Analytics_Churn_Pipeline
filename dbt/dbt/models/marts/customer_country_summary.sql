{{ config(
    materialized='incremental',
    unique_key=['customer_country', 'customer_subscription_tier', 'customer_acquisition_channel'],
    incremental_strategy='append',
    engine='MergeTree()',
    order_by=['customer_country', 'customer_subscription_tier', 'customer_acquisition_channel']
) }}

WITH profiles AS (
    SELECT *
    FROM {{ ref('int_customer_profiles_ml') }}
)

SELECT
    customer_country,
    customer_subscription_tier,
    customer_acquisition_channel,
    countDistinct(customer_id) AS num_customers,
    sum(total_orders) AS total_orders,
    sum(total_spend_usd) AS total_spend_usd,
    sum(orders_last_30_days) AS orders_last_30_days,
    sum(spend_last_30_days_usd) AS spend_last_30_days_usd,
    sum(total_web_events) AS total_web_events,
    sum(sessions) AS unique_sessions,
    avg(days_since_last_purchase) AS avg_days_since_last_order,
    avg(days_since_last_web_activity) AS avg_days_since_last_web_activity,
    avg(days_since_registration) AS avg_days_since_registration,
    now() AS mart_loaded_at
FROM profiles
GROUP BY
    customer_country,
    customer_subscription_tier,
    customer_acquisition_channel
