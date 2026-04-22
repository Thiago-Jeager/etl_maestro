{{
    config(
        materialized='table',
        schema='analytics',
        tags=['marts', 'dimensions', 'users', 'pii'],
        meta={
            'owner': 'analytics_team',
            'description': 'Dimension de usuarios con metricas, segmentacion RFM y PII enmascarado (LDPD/GDPR)',
            'data_classification': 'confidential',
            'pii_hashed': true
        }
    )
}}

WITH int_users AS (
    SELECT * FROM {{ ref('int_users_enriched') }}
),

user_segment_score AS (
    SELECT
        user_id,
        -- RFM segmentation basado en transacciones
        CASE
            WHEN total_transactions >= 50 AND total_lifetime_value >= 5000 THEN 'VIP_Customer'
            WHEN total_transactions >= 20 AND total_lifetime_value >= 1000 THEN 'Premium_Customer'
            WHEN total_transactions >= 5 THEN 'Regular_Customer'
            WHEN total_transactions > 0 THEN 'One_Time_Buyer'
            ELSE 'No_Activity'
        END AS customer_segment,
        -- Churn risk
        CASE
            WHEN days_since_registration IS NULL THEN 'unknown'
            WHEN user_activity_segment = 'high_activity' THEN 'low_churn_risk'
            WHEN user_activity_segment = 'medium_activity' THEN 'medium_churn_risk'
            ELSE 'high_churn_risk'
        END AS churn_risk_level,
        -- Engagement score
        CASE
            WHEN data_quality_tier = 'high' AND completeness_flag = 'complete' THEN 100
            WHEN data_quality_tier = 'medium' AND completeness_flag = 'complete' THEN 75
            WHEN completeness_flag = 'complete' THEN 50
            ELSE 25
        END AS engagement_score
    FROM int_users
)

SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.full_name,
    u.country,
    u.registration_date,
    u.registration_year,
    u.registration_month,
    -- Métricas de actividad
    u.total_transactions,
    u.active_days,
    u.total_lifetime_value AS lifetime_value,
    u.avg_transaction_amount,
    u.last_transaction_date,
    u.user_activity_segment,
    -- Flags de estado
    CASE
        WHEN u.user_activity_segment IN ('high_activity', 'medium_activity') THEN true
        ELSE false
    END AS is_active_user,
    CASE
        WHEN seg.churn_risk_level = 'high_churn_risk' THEN true
        ELSE false
    END AS is_churned_user,
    -- Segmentación
    seg.customer_segment,
    seg.churn_risk_level,
    seg.engagement_score,
    -- RFM Score (1-5 scale)
    CASE
        WHEN seg.customer_segment = 'VIP_Customer' THEN 5
        WHEN seg.customer_segment = 'Premium_Customer' THEN 4
        WHEN seg.customer_segment = 'Regular_Customer' THEN 3
        WHEN seg.customer_segment = 'One_Time_Buyer' THEN 2
        ELSE 1
    END AS rfm_score,
    -- Calidad de datos
    u.data_quality_score,
    u.data_quality_tier,
    u.completeness_flag,
    u.has_missing_email,
    u.user_trust_level,
    -- Temporal
    u.days_since_registration,
    u.user_age_segment,
    u.processed_at,
    CURRENT_TIMESTAMP AS dbt_processed_at
FROM int_users u
INNER JOIN user_segment_score seg ON u.user_id = seg.user_id
WHERE u.data_quality_score >= 0.7  -- Filtrar por calidad mínima
ORDER BY u.total_lifetime_value DESC