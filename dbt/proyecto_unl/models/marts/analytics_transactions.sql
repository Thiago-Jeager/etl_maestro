{{ config(materialized='view', schema='analytics') }}

SELECT
    t.transaction_id,
    pseudonymize_user(t.user_id) AS user_pseudo_id,
    t.product_category,
    t.amount,
    t.currency,
    t.transaction_date,
    t.status,
    t.data_quality_score,
    t.transaction_day,
    t.transaction_quarter,
    t.user_value_quartile,
    t.user_frequency_quartile
FROM {{ ref('fct_transactions') }} AS t