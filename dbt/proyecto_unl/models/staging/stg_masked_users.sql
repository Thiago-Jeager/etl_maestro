{{ config(materialized='view', schema='development') }}

SELECT
    user_id,
    first_name,
    last_name,
    {{ mask_email('email::VARCHAR(255)') }} AS email_masked,
    {{ mask_ip_address('ip_address::VARCHAR(39)') }} AS ip_address_masked,
    country,
    registration_date,
    processed_at
    --data_quality_score
FROM {{ ref('stg_raw_users') }}