{{ config(
    materialized='incremental',
    unique_key=['registration_month'],
    incremental_strategy='append',
    engine='MergeTree()',
    order_by=['registration_month']
) }}

WITH profiles AS (
    SELECT
        *,
        date_trunc('month', customer_registration_date) AS registration_month
    FROM {{ ref('int_customer_profiles_ml') }}
)

SELECT
    registration_month,
    countDistinct(customer_id) AS new_customers,
    sum(total_spend_usd) AS total_spend_usd,
    sum(total_orders) AS total_orders,
    sum(orders_last_30_days) AS orders_last_30_days,
    sum(spend_last_30_days_usd) AS spend_last_30_days_usd,
    sum(total_web_events) AS total_web_events,
    sum(sessions) AS unique_sessions,
    now() AS mart_loaded_at
FROM profiles
GROUP BY registration_month
