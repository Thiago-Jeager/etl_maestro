WITH origen AS (
    SELECT
        transaction_id,
        user_id,
        product_category,
        amount,
        currency,
        transaction_date,
        status,
        processed_at,
        data_quality_score
    FROM {{ source('prod_source', 'raw_transactions') }}
),
calidad_datos AS (
    SELECT
        *,
        -- Calcular puntaje de calidad basado en completitud y validez
        CASE
            WHEN transaction_id IS NOT NULL
                 AND user_id IS NOT NULL
                 AND product_category IS NOT NULL
                 AND amount IS NOT NULL AND amount > 0
                 AND currency IN ('USD', 'EUR', 'GBP', 'JPY')
                 AND status IN ('COMPLETED', 'PENDING', 'FAILED')
                 AND transaction_date IS NOT NULL
            THEN 1.0
            WHEN transaction_id IS NOT NULL
                 AND user_id IS NOT NULL
                 AND amount IS NOT NULL AND amount > 0
                 AND currency IN ('USD', 'EUR', 'GBP', 'JPY')
                 AND status IN ('COMPLETED', 'PENDING', 'FAILED')
            THEN 0.8
            WHEN transaction_id IS NOT NULL
                 AND user_id IS NOT NULL
                 AND amount IS NOT NULL AND amount > 0
            THEN 0.6
            ELSE 0.3
        END AS calculated_data_quality_score,

        -- Clasificar calidad de datos
        CASE
            WHEN transaction_id IS NOT NULL
                 AND user_id IS NOT NULL
                 AND product_category IS NOT NULL
                 AND amount IS NOT NULL AND amount > 0
                 AND currency IN ('USD', 'EUR', 'GBP', 'JPY')
                 AND status IN ('COMPLETED', 'PENDING', 'FAILED')
                 AND transaction_date IS NOT NULL
            THEN 'high'
            WHEN transaction_id IS NOT NULL
                 AND user_id IS NOT NULL
                 AND amount IS NOT NULL AND amount > 0
                 AND currency IN ('USD', 'EUR', 'GBP', 'JPY')
                 AND status IN ('COMPLETED', 'PENDING', 'FAILED')
            THEN 'medium'
            ELSE 'low'
        END AS data_quality_tier,

        -- Clasificar valor de transacción
        CASE
            WHEN amount >= 1000 THEN 'high_value'
            WHEN amount >= 100 THEN 'medium_value'
            ELSE 'low_value'
        END AS transaction_value_tier

    FROM origen
)
SELECT
    transaction_id,
    user_id,
    product_category,
    amount,
    currency,
    transaction_date,
    status,
    processed_at,
    COALESCE(data_quality_score, calculated_data_quality_score) AS data_quality_score,
    data_quality_tier,
    transaction_value_tier
FROM calidad_datos