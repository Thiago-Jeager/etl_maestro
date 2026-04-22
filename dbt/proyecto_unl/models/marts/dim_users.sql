{{
    config(
        materialized='table',
        schema='analytics',
        tags=['marts', 'dimensions'],
        meta={
            'owner': 'analytics_team',
            'description': 'Dimension de usuarios con metricas agregadas y segmentacion RFM',
            'data_classification': 'confidential'
        }
    )
}}

WITH stg AS (SELECT * FROM {{ ref('stg_raw_transactions') }}),

user_profile AS (
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT DATE(transaction_date)) AS active_days,
        COUNT(DISTINCT product_category) AS unique_categories_purchased,
        SUM(amount) AS lifetime_value,
        AVG(amount) AS avg_ticket_size,
        MIN(amount) AS min_transaction,
        MAX(amount) AS max_transaction,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_transaction,
        MIN(transaction_date) AS first_seen_date,
        MAX(transaction_date) AS last_seen_date,
        EXTRACT(DAY FROM NOW() - MAX(transaction_date)) AS days_since_last_transaction,
        NTILE(5) OVER (ORDER BY SUM(amount) DESC) AS value_segment,
        NTILE(5) OVER (ORDER BY COUNT(*) DESC) AS frequency_segment,
        NTILE(5) OVER (ORDER BY MAX(transaction_date) DESC) AS recency_segment
    FROM stg WHERE status = 'COMPLETED'
    GROUP BY user_id
)

SELECT
    user_id, total_transactions, active_days, unique_categories_purchased,
    lifetime_value, avg_ticket_size, min_transaction, max_transaction,
    median_transaction, first_seen_date, last_seen_date,
    days_since_last_transaction,
    value_segment, frequency_segment, recency_segment,
    (value_segment + frequency_segment + recency_segment) / 3.0 AS rfm_score,
    CASE WHEN days_since_last_transaction <= 30 THEN TRUE ELSE FALSE END AS is_active_user,
    CASE WHEN days_since_last_transaction > 30 AND days_since_last_transaction <= 90 THEN TRUE ELSE FALSE END AS is_at_risk_user,
    CASE WHEN days_since_last_transaction > 90 THEN TRUE ELSE FALSE END AS is_churned_user,
    CURRENT_TIMESTAMP AS dbt_processed_at
FROM user_profile
ORDER BY lifetime_value DESC