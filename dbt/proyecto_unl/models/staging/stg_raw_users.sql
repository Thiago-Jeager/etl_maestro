{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'users', 'pii']
    )
}}

WITH source AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        ip_address,
        country,
        registration_date,
        processed_at,
        data_quality_score
    FROM {{ source('prod_source', 'raw_users') }}
),

renamed AS (
    SELECT
        user_id::INTEGER AS user_id,
        INITCAP(LOWER(first_name))::VARCHAR(100) AS first_name,
        INITCAP(LOWER(last_name))::VARCHAR(100) AS last_name,
        CASE
            WHEN email IS NULL THEN NULL
            ELSE LOWER(email)::VARCHAR(255)
        END AS email,
        ip_address::VARCHAR(39) AS ip_address,
        -- PII ENMASCARADO: mantenemos hashes
        country::VARCHAR(30) AS country,
        registration_date::DATE AS registration_date,
        EXTRACT(YEAR FROM registration_date)::INTEGER AS registration_year,
        EXTRACT(MONTH FROM registration_date)::INTEGER AS registration_month,
        ROUND(data_quality_score::NUMERIC(3,2), 2) AS data_quality_score,
        CASE
            WHEN data_quality_score >= 0.9 THEN 'high'
            WHEN data_quality_score >= 0.7 THEN 'medium'
            ELSE 'low'
        END AS data_quality_tier,
        CASE
            WHEN email IS NULL THEN 'missing_email'
            ELSE 'complete'
        END AS completeness_flag,
        processed_at::TIMESTAMP AS processed_at,
        CURRENT_TIMESTAMP AS dbt_transformed_at
    FROM source
)

SELECT
    *,
    CONCAT(first_name, ' ', last_name) AS full_name,
    CASE WHEN completeness_flag = 'complete' THEN 1 ELSE 0 END AS is_complete
FROM renamed
