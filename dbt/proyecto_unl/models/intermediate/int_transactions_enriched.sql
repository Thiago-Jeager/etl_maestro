WITH base_transactions AS (
    SELECT * FROM {{ ref('stg_raw_transactions') }}
),
user_metrics AS (
    -- Métricas históricas por usuario
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        AVG(amount) AS avg_transaction_amount,
        MAX(amount) AS max_transaction_amount,
        MIN(amount) AS min_transaction_amount,
        COUNT(DISTINCT product_category) AS unique_categories,
        MAX(transaction_date) AS last_transaction_date,
        DATE_PART('day', MAX(transaction_date) - MIN(transaction_date)) AS user_lifespan_days,
        AVG(data_quality_score) AS avg_data_quality_score
    FROM base_transactions
    GROUP BY user_id
),
category_metrics AS (
    -- Métricas por categoría de producto
    SELECT
        product_category,
        COUNT(*) AS category_total_transactions,
        AVG(amount) AS category_avg_amount,
        STDDEV(amount) AS category_amount_stddev,
        AVG(data_quality_score) AS category_avg_quality
    FROM base_transactions
    GROUP BY product_category
),
enriched AS (
    SELECT
        t.transaction_id,
        t.user_id,
        t.product_category,
        t.amount,
        t.currency,
        t.transaction_date,
        t.status,
        t.data_quality_score,
        t.data_quality_tier,
        t.transaction_value_tier,

        -- Métricas del usuario
        u.total_transactions,
        u.avg_transaction_amount,
        u.max_transaction_amount,
        u.min_transaction_amount,
        u.unique_categories,
        u.last_transaction_date,
        u.user_lifespan_days,
        u.avg_data_quality_score,

        -- Métricas de la categoría
        c.category_total_transactions,
        c.category_avg_amount,
        c.category_amount_stddev,
        c.category_avg_quality,

        -- Cálculos adicionales
        t.amount / NULLIF(u.avg_transaction_amount, 0) AS amount_vs_user_avg,
        t.amount / NULLIF(c.category_avg_amount, 0) AS amount_vs_category_avg,

        -- Puntaje de confiabilidad compuesto
        CASE
            WHEN t.data_quality_score >= 0.8
                 AND t.amount BETWEEN (u.avg_transaction_amount - u.avg_transaction_amount * 0.5)
                                  AND (u.avg_transaction_amount + u.avg_transaction_amount * 0.5)
                 AND t.amount BETWEEN (c.category_avg_amount - c.category_amount_stddev * 2)
                                  AND (c.category_avg_amount + c.category_amount_stddev * 2)
            THEN 0.9
            WHEN t.data_quality_score >= 0.6
                 AND t.amount BETWEEN (u.avg_transaction_amount - u.avg_transaction_amount * 0.8)
                                  AND (u.avg_transaction_amount + u.avg_transaction_amount * 0.8)
            THEN 0.7
            WHEN t.data_quality_score >= 0.4
            THEN 0.5
            ELSE 0.3
        END AS composite_reliability_score

    FROM base_transactions t
    LEFT JOIN user_metrics u ON t.user_id = u.user_id
    LEFT JOIN category_metrics c ON t.product_category = c.product_category
)
SELECT * FROM enriched