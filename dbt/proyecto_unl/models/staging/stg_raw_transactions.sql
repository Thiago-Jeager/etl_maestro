{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'transactions']
    )
}}

WITH source AS (
    SELECT
        transaction_id, user_id, product_category, amount,
        currency, transaction_date, status,
        processed_at, data_quality_score
    FROM {{ source('prod_source', 'raw_transactions') }}
),

renamed AS (
    SELECT
        transaction_id::VARCHAR(50) AS transaction_id,
        user_id::INTEGER AS user_id,
        LOWER(product_category) AS product_category,
        currency::VARCHAR(3) AS currency,
        ROUND(amount::NUMERIC(10,2), 2) AS amount,
        transaction_date::TIMESTAMP AS transaction_date,
        DATE_TRUNC('day', transaction_date) AS transaction_day,
        EXTRACT(YEAR FROM transaction_date) AS transaction_year,
        EXTRACT(MONTH FROM transaction_date) AS transaction_month,
        EXTRACT(DAY FROM transaction_date) AS transaction_day_of_month,
        EXTRACT(DOW FROM transaction_date) AS transaction_day_of_week,
        EXTRACT(HOUR FROM transaction_date) AS transaction_hour,
        UPPER(status)::VARCHAR(20) AS status,
        processed_at::TIMESTAMP AS processed_at,
        ROUND(data_quality_score::NUMERIC(3,2), 2) AS data_quality_score,
        CASE
            WHEN data_quality_score >= 0.9 THEN 'high'
            WHEN data_quality_score >= 0.7 THEN 'medium'
            ELSE 'low'
        END AS data_quality_tier,
        CASE
            WHEN amount >= 1000 THEN 'high_value'
            WHEN amount >= 100 THEN 'medium_value'
            ELSE 'low_value'
        END AS transaction_value_tier,
        CURRENT_TIMESTAMP AS dbt_transformed_at
    FROM source
)

SELECT
    *,
    CASE WHEN amount < 0 THEN TRUE ELSE FALSE END AS is_anomaly,
    CASE WHEN transaction_day_of_week IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM renamed