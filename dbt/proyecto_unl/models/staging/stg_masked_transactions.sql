{{ config(materialized='view', schema='development') }}

SELECT
    transaction_id,
    {{ mask_id('user_id::TEXT') }} AS user_id_masked,
    product_category,
    amount,
    currency,
    transaction_date,
    status
FROM {{ ref('stg_raw_transactions') }}