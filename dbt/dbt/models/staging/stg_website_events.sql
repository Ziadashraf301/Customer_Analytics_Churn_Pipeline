{{
    config(
        materialized='incremental',
        unique_key='event_id',
        cluster_by=['event_timestamp_utc'],
        incremental_strategy='delete+insert',
    )
}}

WITH base AS (

    SELECT
        toString(user_id) AS customer_id,
        toDateTime(event_timestamp) AS event_timestamp_utc,
        toString(event_type) AS event_type,
        toString(page_url) AS page_url,
        toString(product_id) AS product_id,
        toString(session_id) AS session_id,
        toString(device_type) AS device_type,
        toString(search_query) AS search_query,
        toDateTime(_processed_at) AS stream_processed_at_utc,

        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['user_id', 'event_timestamp', 'session_id']) }} AS event_id,

        now() AS dbt_loaded_at
    FROM {{ source('raw_stream', 'website_events_stream_flink') }}
    {% if is_incremental() %}
    WHERE _processed_at > (SELECT max(stream_processed_at_utc) FROM {{ this }})
    {% endif %}

),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY event_id ORDER BY event_timestamp_utc DESC) AS rn
        FROM base
    ) t
    WHERE rn = 1
)

SELECT *
FROM deduped
