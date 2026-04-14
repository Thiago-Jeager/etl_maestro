WITH base_transactions AS (
    SELECT * FROM {{ ref('stg_raw_transactions') }}
),
user_summary AS (
    -- Resumen completo por usuario
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_transactions,
        COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_transactions,
        COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_transactions,

        -- Métricas financieras
        SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS lifetime_value,
        AVG(CASE WHEN status = 'COMPLETED' THEN amount ELSE NULL END) AS avg_transaction_value,
        MAX(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS max_transaction_value,
        MIN(CASE WHEN status = 'COMPLETED' THEN amount ELSE NULL END) AS min_transaction_value,

        -- Métricas temporales
        MAX(transaction_date) AS last_transaction_date,
        MIN(transaction_date) AS first_transaction_date,
        DATE_PART('day', MAX(transaction_date) - MIN(transaction_date)) AS user_lifespan_days,
        DATE_PART('day', CURRENT_DATE - MAX(transaction_date)) AS days_since_last_transaction,

        -- Métricas de diversidad
        COUNT(DISTINCT product_category) AS unique_categories,
        COUNT(DISTINCT currency) AS unique_currencies,

        -- Métricas de calidad
        AVG(data_quality_score) AS avg_data_quality_score,
        AVG(CASE WHEN data_quality_tier = 'high' THEN 1
                 WHEN data_quality_tier = 'medium' THEN 0.5
                 ELSE 0 END) AS quality_score_numeric,

        -- Frecuencia mensual
        COUNT(*) * 1.0 / NULLIF((DATE_PART('year', AGE(MAX(transaction_date), MIN(transaction_date))) * 12 + DATE_PART('month', AGE(MAX(transaction_date), MIN(transaction_date)))) + 1, 0) AS monthly_transaction_rate

    FROM base_transactions
    GROUP BY user_id
),
rfm_calculation AS (
    SELECT
        *,
        -- Calcular Recency (R): días desde la última transacción
        CASE
            WHEN days_since_last_transaction <= 7 THEN 5
            WHEN days_since_last_transaction <= 14 THEN 4
            WHEN days_since_last_transaction <= 30 THEN 3
            WHEN days_since_last_transaction <= 90 THEN 2
            ELSE 1
        END AS recency_score,

        -- Calcular Frequency (F): frecuencia de transacciones
        CASE
            WHEN monthly_transaction_rate >= 4 THEN 5
            WHEN monthly_transaction_rate >= 2 THEN 4
            WHEN monthly_transaction_rate >= 1 THEN 3
            WHEN monthly_transaction_rate >= 0.5 THEN 2
            ELSE 1
        END AS frequency_score,

        -- Calcular Monetary (M): valor monetario
        CASE
            WHEN lifetime_value >= 5000 THEN 5
            WHEN lifetime_value >= 2000 THEN 4
            WHEN lifetime_value >= 1000 THEN 3
            WHEN lifetime_value >= 500 THEN 2
            ELSE 1
        END AS monetary_score,

        -- RFM Score compuesto
        (
            CASE
                WHEN days_since_last_transaction <= 7 THEN 5
                WHEN days_since_last_transaction <= 14 THEN 4
                WHEN days_since_last_transaction <= 30 THEN 3
                WHEN days_since_last_transaction <= 90 THEN 2
                ELSE 1
            END +
            CASE
                WHEN monthly_transaction_rate >= 4 THEN 5
                WHEN monthly_transaction_rate >= 2 THEN 4
                WHEN monthly_transaction_rate >= 1 THEN 3
                WHEN monthly_transaction_rate >= 0.5 THEN 2
                ELSE 1
            END +
            CASE
                WHEN lifetime_value >= 5000 THEN 5
                WHEN lifetime_value >= 2000 THEN 4
                WHEN lifetime_value >= 1000 THEN 3
                WHEN lifetime_value >= 500 THEN 2
                ELSE 1
            END
        ) / 3.0 AS rfm_score

    FROM user_summary
),
user_segmentation AS (
    SELECT
        *,
        -- Determinar si es usuario activo (transacción en los últimos 30 días)
        CASE WHEN days_since_last_transaction <= 30 THEN TRUE ELSE FALSE END AS is_active_user,

        -- Determinar si es usuario churned (sin transacción en los últimos 90 días)
        CASE WHEN days_since_last_transaction > 90 THEN TRUE ELSE FALSE END AS is_churned_user,

        -- Segmentación RFM
        CASE
            WHEN rfm_score >= 4.5 THEN 'Champion'
            WHEN rfm_score >= 4.0 THEN 'Loyal'
            WHEN rfm_score >= 3.5 THEN 'Potential'
            WHEN rfm_score >= 3.0 THEN 'Promising'
            WHEN rfm_score >= 2.5 THEN 'Needs Attention'
            ELSE 'At Risk'
        END AS rfm_segment,

        -- Categorización por valor
        CASE
            WHEN lifetime_value >= 5000 THEN 'VIP'
            WHEN lifetime_value >= 2000 THEN 'High Value'
            WHEN lifetime_value >= 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_category,

        -- Timestamp de procesamiento
        CURRENT_TIMESTAMP AS updated_at

    FROM rfm_calculation
)
SELECT
    user_id,
    total_transactions,
    completed_transactions,
    failed_transactions,
    pending_transactions,
    lifetime_value,
    avg_transaction_value,
    max_transaction_value,
    min_transaction_value,
    last_transaction_date,
    first_transaction_date,
    user_lifespan_days,
    days_since_last_transaction,
    unique_categories,
    unique_currencies,
    avg_data_quality_score,
    quality_score_numeric,
    monthly_transaction_rate,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    is_active_user,
    is_churned_user,
    rfm_segment,
    value_category,
    updated_at
FROM user_segmentation