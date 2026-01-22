use default;

CREATE TABLE stg_customers
(
    customer_id String,
    customer_registration_date Date,
    customer_subscription_tier String,
    customer_acquisition_channel String,
    customer_email String,
    customer_country String,
    clickhouse_loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (customer_id);

INSERT INTO stg_customers
SELECT
    CAST(user_id AS String) AS customer_id,
    CAST(registration_date AS Date) AS customer_registration_date,
    lower(subscription_tier) AS customer_subscription_tier,
    lower(acquisition_channel) AS customer_acquisition_channel,
    email AS customer_email,
    country AS customer_country,
    now() AS clickhouse_loaded_at
FROM file('/var/lib/clickhouse/user_files/raw_data/customer_profiles.parquet', 'Parquet');
