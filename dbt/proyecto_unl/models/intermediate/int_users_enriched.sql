{{
    config(
        materialized='view',
        schema='intermediate',
        tags=['intermediate', 'users', 'enrichment', 'pii']
    )
}}

WITH stg_users AS (
    SELECT * FROM {{ ref('stg_raw_users') }}
),

stg_tx AS (
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT DATE(transaction_date)) AS active_days,
        SUM(amount) AS total_transaction_value,
        AVG(amount) AS avg_transaction_amount,
        MAX(transaction_date) AS last_transaction_date,
        CASE
            WHEN COUNT(*) >= 50 THEN 'high_activity'
            WHEN COUNT(*) >= 10 THEN 'medium_activity'
            ELSE 'low_activity'
        END AS user_activity_segment
    FROM {{ ref('stg_raw_transactions') }}
    WHERE status = 'COMPLETED'
    GROUP BY user_id
),

user_enriched AS (
    SELECT
        u.user_id,
        u.first_name,
        u.last_name,
        u.full_name,
        u.email,       
        u.ip_address,
        u.email IS NULL AS has_missing_email,  -- Banderas de completitud
        u.country,
        u.registration_date,
        u.registration_year,
        u.registration_month,
        -- Métricas de transacciones
        COALESCE(tx.total_transactions, 0) AS total_transactions,
        COALESCE(tx.active_days, 0) AS active_days,
        COALESCE(tx.total_transaction_value, 0)::NUMERIC(15,2) AS total_lifetime_value,
        COALESCE(tx.avg_transaction_amount, 0)::NUMERIC(10,2) AS avg_transaction_amount,
        tx.last_transaction_date,
        COALESCE(tx.user_activity_segment, 'no_activity') AS user_activity_segment,
        -- Calidad de datos
        u.data_quality_score,
        u.data_quality_tier,
        u.completeness_flag,
        -- Timestamps
        u.processed_at,
        u.dbt_transformed_at,
        CURRENT_TIMESTAMP AS dbt_enriched_at
    FROM stg_users u
    LEFT JOIN stg_tx tx ON u.user_id = tx.user_id
),

final_enrichment AS (
    SELECT
        *,
        CASE
            WHEN data_quality_score >= 0.9 AND completeness_flag = 'complete' THEN 'trusted'
            WHEN data_quality_score >= 0.7 THEN 'acceptable'
            ELSE 'needs_review'
        END AS user_trust_level,
        -- Cálculo de días desde registro
        (CURRENT_DATE - registration_date)::INTEGER AS days_since_registration
    FROM user_enriched
)

SELECT
    *,
    CASE
        WHEN days_since_registration <= 30 THEN 'new_user'
        WHEN days_since_registration <= 180 THEN 'recent_user'
        ELSE 'established_user'
    END AS user_age_segment
FROM final_enrichment
