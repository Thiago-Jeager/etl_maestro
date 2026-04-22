{{
    config(
        materialized='table',
        schema='analytics',
        tags=['marts', 'users'],
        meta={
            'description': 'Tabla maestra de usuarios con métricas agregadas y niveles de confianza'
        }
    )
}}

WITH enriched AS (
    SELECT * FROM {{ ref('int_users_enriched') }}
),

final AS (
    SELECT
        user_id,
        first_name,
        last_name,
        full_name,
        email,
        ip_address,
        country,
        registration_date,
        registration_year,
        registration_month,
        
        -- Métricas de Actividad
        total_transactions,
        active_days,
        total_lifetime_value,
        avg_transaction_amount,
        last_transaction_date,
        user_activity_segment,
        
        -- Atributos de Segmentación
        user_age_segment,
        user_trust_level,
        
        -- Calidad
        data_quality_score,
        data_quality_tier,
        
        -- Auditoría
        processed_at,
        dbt_enriched_at as transformed_at
    FROM enriched
    -- Filtramos usuarios que no son confiables o no tienen transacciones si es para un fct_activo
    -- O simplemente lo dejamos todo si queremos una dimensión completa
    WHERE user_trust_level IN ('trusted', 'acceptable')
)

SELECT * FROM final