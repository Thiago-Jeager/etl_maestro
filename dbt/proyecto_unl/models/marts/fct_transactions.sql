{{
    config(
        materialized='table',
        schema='analytics',
        tags=['marts', 'facts', 'daily'],
        meta={
            'owner': 'analytics_team',
            'description': 'Tabla de hechos de transacciones lista para dashboards y analisis',
            'update_frequency': 'daily',
            'data_classification': 'internal',
            'pii_contains': 'user_id'
        }
    )
}}

WITH enriched AS (SELECT * FROM {{ ref('int_transactions_enriched') }}),

high_quality_only AS (
    SELECT * FROM enriched
    WHERE data_quality_score >= 0.7 AND status = 'COMPLETED' AND amount > 0
),

date_dims AS (
    SELECT *,
        CASE
            WHEN transaction_month IN (1,2,3) THEN 'Q1'
            WHEN transaction_month IN (4,5,6) THEN 'Q2'
            WHEN transaction_month IN (7,8,9) THEN 'Q3'
            ELSE 'Q4'
        END AS transaction_quarter,
        CASE WHEN transaction_month <= 6 THEN 'H1' ELSE 'H2' END AS transaction_semester,
        CASE
            WHEN transaction_day_of_week = 0 THEN 'Domingo'
            WHEN transaction_day_of_week = 1 THEN 'Lunes'
            WHEN transaction_day_of_week = 2 THEN 'Martes'
            WHEN transaction_day_of_week = 3 THEN 'Miercoles'
            WHEN transaction_day_of_week = 4 THEN 'Jueves'
            WHEN transaction_day_of_week = 5 THEN 'Viernes'
            WHEN transaction_day_of_week = 6 THEN 'Sabado'
        END AS transaction_day_name,
        CASE WHEN transaction_day_of_week IN (0, 6) THEN 'Fin de Semana' ELSE 'Dia Laboral' END AS day_type
    FROM high_quality_only
)

SELECT
    transaction_id, user_id, product_category, currency, status,
    transaction_value_tier, data_quality_tier,
    transaction_date, transaction_day, transaction_day_of_week,
    transaction_day_name, transaction_month, transaction_year,
    transaction_quarter, transaction_semester, day_type,
    amount, data_quality_score, composite_reliability_score,
    user_total_transactions, user_total_amount, user_avg_transaction_amount,
    user_active_days, user_value_quartile, user_frequency_quartile,
    category_avg_amount, daily_transaction_count, daily_total_amount,
    daily_amount_7day_moving_avg,
    is_anomaly, is_weekend, dbt_transformed_at
FROM date_dims
ORDER BY transaction_date DESC
