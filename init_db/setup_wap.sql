-- Crear esquemas lógicos  para separar las zonas de confianza
CREATE SCHEMA IF NOT EXISTS audit; -- Zona de cuarentea (datos crudos sin validar)
CREATE SCHEMA IF NOT EXISTS prod; --  Zona de producción (datos validados y limpios)

-- TABLA DE AUDITORIA (Write Phase)
CREATE TABLE IF NOT EXISTS audit.raw_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id INT NOT NULL,
    product_category VARCHAR(50),
    amount NUMERIC(10, 2),
    currency VARCHAR(10) DEFAULT 'USD',
    transaction_date TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('COMPLETED', 'PENDING', 'FAILED')),
    -- Metadatos de ingesta para la trazabildiad
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    -- Columnas para Great Expectations
    gx_validation_status VARCHAR(20) DEFAULT 'PENDING',
    gx_validation_errors TEXT
);

-- Indices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_audit_amount ON audit.raw_transactions (amount);
CREATE INDEX IF NOT EXISTS idx_audit_date ON audit.raw_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_audit_status ON audit.raw_transactions (status);

-- TABLA DE PRODUCCION (Publish Phase)
CREATE TABLE IF NOT EXISTS prod.raw_transactions (
    LIKE audit.raw_transactions INCLUDING ALL,
    -- Columnas adicionales para la zona de producción
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score NUMERIC(3, 2)
);

-- TABLA DE LOGS DE VALIDACION DE DATOS (Great Expectations)
CREATE TABLE IF NOT EXISTS audit.gx_validation_logs (
    log_id SERIAL PRIMARY KEY,
    validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100) NOT NULL,
    expectation_suite_name VARCHAR(100),
    total_records INT,
    failed_records INT,
    success_rate NUMERIC(5, 2),
    critical_failures TEXT[],
    warning_messages TEXT[]
);

-- VISTA DE MONITOREO DE CALIDAD DE DATOS EN TIEMPO REAL
CREATE OR REPLACE VIEW audit.vw_data_quality_dashboard AS
SELECT
    DATE(ingested_at) AS fecha,
    COUNT(*) AS total_registros,
    COUNT(CASE WHEN amount < 0 THEN 1 END) AS registros_monto_negativo,
    COUNT(CASE WHEN amount IS NULL THEN 1 END) AS registros_monto_nulo,
    COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS registros_user_nulo,
    ROUND(
        100.0 * COUNT(CASE WHEN amount >= 0 AND amount IS NOT NULL THEN 1 END) / NULLIF(COUNT(*), 0), 2
    ) AS porcentaje_calidad
FROM audit.raw_transactions
GROUP BY DATE(ingested_at)
ORDER BY fecha DESC;

-- PERMISOS BÁSICOS (Ajusten según su modelo de seguridad)
GRANT USAGE ON SCHEMA audit, prod TO user_dbt;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA audit TO user_dbt;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA prod TO user_dbt;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit TO user_dbt;

-- Mensaje de confirmación
DO $$
BEGIN
    RAISE NOTICE 'Esquemas y tablas para WAP creados exitosamente.';
END $$;