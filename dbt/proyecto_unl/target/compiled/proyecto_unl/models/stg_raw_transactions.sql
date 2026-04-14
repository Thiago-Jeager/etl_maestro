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
    FROM "db_warehouse"."prod"."raw_transactions"
)
SELECT * FROM origen