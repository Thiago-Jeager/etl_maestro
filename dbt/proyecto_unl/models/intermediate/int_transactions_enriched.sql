{{
    config(
        materialized='view',
        schema='intermediate',
        tags=['intermediate', 'enrichment']
    )
}}

WITH stg AS (SELECT * FROM {{ ref('stg_raw_transactions') }}),

user_metrics AS (
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT DATE(transaction_date)) AS active_days,
        COUNT(DISTINCT product_category) AS unique_categories,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_transaction_amount,
        MIN(amount) AS min_transaction_amount,
        MAX(amount) AS max_transaction_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_transaction_amount,
        STDDEV(amount) AS stddev_transaction_amount,
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        NTILE(4) OVER (ORDER BY SUM(amount) DESC) AS user_value_quartile,
        NTILE(4) OVER (ORDER BY COUNT(*) DESC) AS user_frequency_quartile
    FROM stg WHERE status = 'COMPLETED'
    GROUP BY user_id
),

category_metrics AS (
    SELECT
        product_category,
        COUNT(*) AS category_total_transactions,
        SUM(amount) AS category_total_amount,
        AVG(amount) AS category_avg_amount,
        COUNT(DISTINCT user_id) AS category_unique_users
    FROM stg WHERE status = 'COMPLETED'
    GROUP BY product_category
),

daily_metrics AS (
    SELECT
        transaction_day,
        COUNT(*) AS daily_transaction_count,
        SUM(amount) AS daily_total_amount,
        AVG(amount) AS daily_avg_amount,
        COUNT(DISTINCT user_id) AS daily_unique_users,
        AVG(SUM(amount)) OVER (
            ORDER BY transaction_day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS daily_amount_7day_moving_avg
    FROM stg WHERE status = 'COMPLETED'
    GROUP BY transaction_day
)

SELECT
    t.*,
    um.total_transactions AS user_total_transactions,
    um.active_days AS user_active_days,
    um.unique_categories AS user_unique_categories,
    um.total_amount AS user_total_amount,
    um.avg_transaction_amount AS user_avg_transaction_amount,
    um.median_transaction_amount AS user_median_transaction_amount,
    um.stddev_transaction_amount AS user_stddev_transaction_amount,
    um.user_value_quartile,
    um.user_frequency_quartile,
    cm.category_total_transactions,
    cm.category_total_amount,
    cm.category_avg_amount,
    cm.category_unique_users,
    dm.daily_transaction_count,
    dm.daily_total_amount,
    dm.daily_avg_amount,
    dm.daily_unique_users,
    dm.daily_amount_7day_moving_avg,
    CASE WHEN um.avg_transaction_amount > 0
        THEN ROUND((t.amount - um.avg_transaction_amount) / um.avg_transaction_amount * 100, 2)
        ELSE NULL
    END AS deviation_from_user_avg_pct,
    CASE WHEN dm.daily_avg_amount > 0
        THEN ROUND((t.amount - dm.daily_avg_amount) / dm.daily_avg_amount * 100, 2)
        ELSE NULL
    END AS deviation_from_daily_avg_pct,
    ROUND(
        (t.data_quality_score * 0.4) +
        (CASE WHEN um.total_transactions > 10 THEN 0.3 ELSE 0.1 END) +
        (CASE WHEN t.amount > cm.category_avg_amount THEN 0.3 ELSE 0.1 END)
    , 2) AS composite_reliability_score
FROM stg t
LEFT JOIN user_metrics um ON t.user_id = um.user_id
LEFT JOIN category_metrics cm ON t.product_category = cm.product_category
LEFT JOIN daily_metrics dm ON t.transaction_day = dm.transaction_day


