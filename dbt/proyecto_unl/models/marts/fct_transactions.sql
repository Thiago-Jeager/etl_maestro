WITH enriched_transactions AS (
    SELECT * FROM {{ ref('int_transactions_enriched') }}
),
anomaly_detection AS (
    SELECT
        *,
        -- Detectar anomalías basadas en reglas de negocio
        CASE
            WHEN composite_reliability_score < 0.5 THEN TRUE
            WHEN amount_vs_user_avg > 3 OR amount_vs_user_avg < 0.33 THEN TRUE
            WHEN amount_vs_category_avg > 2.5 OR amount_vs_category_avg < 0.4 THEN TRUE
            WHEN data_quality_score < 0.4 THEN TRUE
            WHEN amount > 5000 AND transaction_value_tier = 'low_value' THEN TRUE
            WHEN status = 'FAILED' AND data_quality_score > 0.8 THEN TRUE
            ELSE FALSE
        END AS is_anomaly,

        -- Categorizar tipo de anomalía
        CASE
            WHEN composite_reliability_score < 0.5 THEN 'low_reliability'
            WHEN amount_vs_user_avg > 3 OR amount_vs_user_avg < 0.33 THEN 'amount_outlier'
            WHEN amount_vs_category_avg > 2.5 OR amount_vs_category_avg < 0.4 THEN 'category_outlier'
            WHEN data_quality_score < 0.4 THEN 'data_quality_issue'
            WHEN amount > 5000 AND transaction_value_tier = 'low_value' THEN 'value_mismatch'
            WHEN status = 'FAILED' AND data_quality_score > 0.8 THEN 'status_inconsistency'
            ELSE 'normal'
        END AS anomaly_type

    FROM enriched_transactions
),
final_metrics AS (
    SELECT
        transaction_id,
        user_id,
        product_category,
        amount,
        currency,
        transaction_date,
        status,
        data_quality_score,
        composite_reliability_score,
        is_anomaly,
        anomaly_type,

        -- Métricas adicionales para análisis
        CASE
            WHEN status = 'COMPLETED' THEN amount
            ELSE 0
        END AS completed_amount,

        CASE
            WHEN status = 'PENDING' THEN 1
            ELSE 0
        END AS is_pending,

        CASE
            WHEN status = 'FAILED' THEN 1
            ELSE 0
        END AS is_failed,

        -- Segmentación por valor
        CASE
            WHEN amount >= 1000 THEN 'premium'
            WHEN amount >= 500 THEN 'high'
            WHEN amount >= 100 THEN 'medium'
            ELSE 'low'
        END AS value_segment,

        -- Indicadores de calidad
        CASE
            WHEN data_quality_score >= 0.8 THEN 'excellent'
            WHEN data_quality_score >= 0.6 THEN 'good'
            WHEN data_quality_score >= 0.4 THEN 'fair'
            ELSE 'poor'
        END AS quality_rating,

        -- Timestamp de procesamiento
        CURRENT_TIMESTAMP AS processed_at

    FROM anomaly_detection
)
SELECT * FROM final_metrics