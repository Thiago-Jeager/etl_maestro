{{
    config(
        materialized='view',
        schema='analytics',
        tags=['analytics', 'public']
    )
}}

SELECT
    -- Pseudonimizamos el ID para analistas externos si es necesario
    -- O usamos el ID real si es interno pero ocultamos nombres
    user_id, 
    
    -- Enmascaramos el nombre completo para reportes generales
    CASE 
        WHEN LENGTH(full_name) > 2 THEN CONCAT(LEFT(full_name, 1), '****')
        ELSE '****'
    END AS user_display_name,
    pseudonymize_email_ip(email) AS pseudo_email,
    pseudonymize_email_ip(ip_address) AS pseudo_ip_address,
    country,
    user_age_segment,
    user_activity_segment,
    user_trust_level,
    
    -- Métricas de negocio (KPIs)
    total_transactions,
    total_lifetime_value,
    avg_transaction_amount,
    active_days,
    
    -- Fechas truncadas para análisis de cohortes
    DATE_TRUNC('month', registration_date) AS cohort_month,
    last_transaction_date
FROM {{ ref('fct_users') }}