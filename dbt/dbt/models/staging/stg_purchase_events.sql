{{ 
    config(
        materialized='incremental',
        unique_key='order_id', 
        cluster_by=['order_purchase_timestamp_utc'],
        incremental_strategy='delete+insert',
    ) 
}}

WITH base AS (

    SELECT
        toString(order_id) AS order_id,
        toString(user_id) AS customer_id,
        toDateTime(purchase_time) AS order_purchase_timestamp_utc,
        toDecimal32(total_amount, 2) AS order_total_amount_usd,
        toString(product_ids) AS order_product_ids_json, -- Store as JSON string
        toString(payment_method) AS order_payment_method,
        toString(shipping_address) AS order_shipping_address,
        toString(order_status) AS order_status,
        toString(currency) AS order_currency,
        toDateTime(_processed_at) AS stream_processed_at_utc, -- Timestamp when Flink processed it
        now() AS dbt_loaded_at -- Timestamp of when this dbt model was run

    FROM {{ source('raw_stream', 'purchase_events_stream_flink') }}
    {% if is_incremental() %}
    WHERE _processed_at > (SELECT max(stream_processed_at_utc) FROM {{ this }})
    {% endif %}

),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp_utc DESC) AS rn
        FROM base
    ) t
    WHERE rn = 1
)

SELECT *
FROM deduped
