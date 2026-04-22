-- Crear esquemas lógicos  para separar las zonas de confianza
CREATE SCHEMA IF NOT EXISTS audit; -- Zona de cuarentea (datos crudos sin validar)
CREATE SCHEMA IF NOT EXISTS prod; --  Zona de producción (datos validados y limpios)
CREATE SCHEMA IF NOT EXISTS analytics; -- Esquema analítico
-- Crear roles si no existen
CREATE ROLE user_dev WITH LOGIN PASSWORD 'password_dev';
CREATE ROLE user_analyst WITH LOGIN PASSWORD 'password_analyst';

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

-- ========================== TABLAS DE USUARIOS (WAP) ==========================

-- TABLA DE AUDITORIA PARA USUARIOS (Write Phase)
CREATE TABLE IF NOT EXISTS audit.raw_users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),  -- Puede ser nulo (5% inyectado intencionalmente)
    ip_address VARCHAR(45) NOT NULL,  -- IPv4 o IPv6
    country VARCHAR(50) NOT NULL,
    registration_date DATE NOT NULL,
    -- Metadatos de ingesta
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    -- Columnas para Great Expectations
    gx_validation_status VARCHAR(20) DEFAULT 'PENDING',
    gx_validation_errors TEXT
);

-- Indices para optimizar consultas de usuarios
CREATE INDEX IF NOT EXISTS idx_audit_users_email ON audit.raw_users (email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_users_country ON audit.raw_users (country);
CREATE INDEX IF NOT EXISTS idx_audit_users_registration_date ON audit.raw_users (registration_date);

-- TABLA DE PRODUCCION PARA USUARIOS (Publish Phase)
CREATE TABLE IF NOT EXISTS prod.raw_users (
    LIKE audit.raw_users INCLUDING ALL,
    -- Columnas adicionales para la zona de producción
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score NUMERIC(3, 2)
);

-- TABLA DE LOGS DE VALIDACION PARA USUARIOS
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

ALTER TABLE audit.gx_validation_logs
ADD COLUMN IF NOT EXISTS blocking_triggered BOOLEAN DEFAULT FALSE;

-- FRESCURA

CREATE TABLE IF NOT EXISTS audit.data_freshness_monitor (
    monitor_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    expected_arrival_time TIMESTAMP,
    actual_arrival_time TIMESTAMP,
    freshness_lag INTERVAL,
    status VARCHAR(20) CHECK (status IN ('FRESH', 'STALE', 'MISSING')),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    threshold_minutes INT
);

CREATE OR REPLACE VIEW audit.vw_freshness_dashboard AS
SELECT
    table_name,
    MAX(actual_arrival_time) as last_data_arrival,
    EXTRACT(EPOCH FROM (NOW() - MAX(actual_arrival_time)))/60 as minutes_since_last_update,
    CASE
        WHEN MAX(actual_arrival_time) > NOW() - INTERVAL '2 hours' THEN 'FRESH'
        WHEN MAX(actual_arrival_time) > NOW() - INTERVAL '6 hours' THEN 'STALE'
        ELSE 'MISSING'
    END as freshness_status
FROM audit.data_freshness_monitor
GROUP BY table_name;

-- VOLUMEN / METRICAS
CREATE TABLE IF NOT EXISTS audit.pipeline_metrics (
    metric_id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100),
    metric_value NUMERIC,
    metric_type VARCHAR(20),
    metadata JSONB,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ESQUEMA
CREATE TABLE IF NOT EXISTS audit.schema_history (
    history_id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    schema_json JSONB NOT NULL,
    drift_detected BOOLEAN DEFAULT FALSE,
    changes_json JSONB,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

-- LINAJE OPERATIVO
CREATE TABLE IF NOT EXISTS audit.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    lineage_type VARCHAR(10) CHECK (lineage_type IN ('node', 'edge')),
    node_id VARCHAR(255),
    node_name VARCHAR(255),
    node_type VARCHAR(50),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    description TEXT,
    source_id VARCHAR(255),
    target_id VARCHAR(255),
    transformation TEXT,
    dag_execution_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW audit.vw_data_lineage AS
SELECT
    n1.node_name AS source,
    e.transformation,
    n2.node_name AS target,
    n1.node_type AS source_type,
    n2.node_type AS target_type
FROM audit.data_lineage e
JOIN audit.data_lineage n1 ON e.source_id = n1.node_id
JOIN audit.data_lineage n2 ON e.target_id = n2.node_id
WHERE e.lineage_type = 'edge';

-- REGISTRO DE PII POR TABLA
CREATE TABLE IF NOT EXISTS audit.pii_catalog (
    catalog_id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    pii_category VARCHAR(50),
    pii_sensitivity VARCHAR(20) CHECK (pii_sensitivity IN ('low', 'medium', 'high', 'critical')),
    protection_method VARCHAR(50),
    access_restriction VARCHAR(100),
    retention_days INT,
    legal_basis VARCHAR(200),
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (schema_name, table_name, column_name)
);



-- Insertar catalogo inicial
INSERT INTO audit.pii_catalog
(schema_name, table_name, column_name, pii_category, pii_sensitivity, protection_method, access_restriction, retention_days, legal_basis)
VALUES
('audit', 'raw_transactions', 'user_id', 'direct_identifier', 'high', 'hash_salting', 'production_only', 365, 'Consentimiento del usuario - LDPD Art. 8'),
('prod', 'raw_transactions', 'user_id', 'direct_identifier', 'high', 'hash_salting', 'production_only', 365, 'Consentimiento del usuario - LDPD Art. 8'),
('staging', 'stg_raw_transactions', 'user_id', 'direct_identifier', 'high', 'hash_salting', 'production_only', 365, 'Consentimiento del usuario - LDPD Art. 8'),
('audit', 'raw_users', 'email', 'contact_information', 'high', 'hash_sha256', 'restricted', 180, 'LDPD Art. 12 - Consentimiento'),
('audit', 'raw_users', 'ip_address', 'device_identifier', 'medium', 'hash_sha256', 'restricted', 90, 'LDPD Art. 12 - Auditoría de seguridad'),
('prod', 'raw_users', 'email', 'contact_information', 'high', 'hash_sha256', 'restricted', 180, 'LDPD Art. 12 - Consentimiento'),
('prod', 'raw_users', 'ip_address', 'device_identifier', 'medium', 'hash_sha256', 'restricted', 90, 'LDPD Art. 12 - Auditoría de seguridad'),
('staging', 'stg_raw_users', 'email', 'contact_information', 'high', 'hash_sha256', 'restricted', 180, 'LDPD Art. 12 - Consentimiento'),
('staging', 'stg_raw_users', 'ip_address', 'device_identifier', 'medium', 'hash_sha256', 'restricted', 90, 'LDPD Art. 12 - Auditoría de seguridad')
ON CONFLICT (schema_name, table_name, column_name) DO NOTHING;

-- Vista de consulta
CREATE OR REPLACE VIEW audit.vw_pii_catalog AS
SELECT
    schema_name || '.' || table_name AS full_table,
    column_name,
    pii_category,
    pii_sensitivity,
    protection_method,
    access_restriction,
    retention_days,
    legal_basis
FROM audit.pii_catalog
ORDER BY pii_sensitivity DESC, schema_name, table_name;


-- VISTA ENMASCARADA PARA ENTORNOS NO PRODUCTIVOS
-- Los analistas de desarrollo ven datos ofuscados
CREATE OR REPLACE VIEW audit.v_masked_transactions AS
SELECT
    transaction_id,
    CONCAT('user_', MD5(user_id::TEXT)) AS user_id_masked,
    product_category,
    amount,
    currency,
    transaction_date,
    status
    -- data_quality_score
FROM audit.raw_transactions;

-- VISTAS ENMASCARADAS PARA USUARIOS
-- Vista para desarrolladores (audit.raw_users - datos sin procesar)
CREATE OR REPLACE VIEW audit.v_masked_users AS
SELECT
    user_id,
    CONCAT('dev_', MD5(first_name::TEXT)) AS first_name_masked,
    CONCAT('dev_', MD5(last_name::TEXT)) AS last_name_masked,
    CONCAT('user_', MD5(COALESCE(email, 'NULL')::TEXT)) AS email_masked,
    CONCAT('ip_', MD5(ip_address::TEXT)) AS ip_address_masked,
    country,
    registration_date
FROM audit.raw_users;

-- Vista para producción (prod.raw_users - datos procesados y validados)
CREATE OR REPLACE VIEW prod.v_masked_users AS
SELECT
    user_id,
    CONCAT('U', LEFT(MD5(first_name::TEXT), 8)) AS first_name_short,
    CONCAT('U', LEFT(MD5(last_name::TEXT), 8)) AS last_name_short,
    email,
    ip_address,
    country,
    registration_date,
    data_quality_score
FROM prod.raw_users;

-- VISTA ENMASCARADA PARA PRODUCCION (analistas de negocio)
CREATE OR REPLACE VIEW prod.v_masked_transactions AS
SELECT
    transaction_id,
    CONCAT('U', LEFT(MD5(user_id::TEXT), 8)) AS user_short_id,
    product_category,
    amount,
    currency,
    DATE_TRUNC('day', transaction_date) AS transaction_day,
    status
FROM prod.raw_transactions;

-- Grants diferenciados para usuarios
GRANT SELECT ON audit.v_masked_users TO user_dev;
GRANT SELECT ON prod.v_masked_users TO user_analyst;
GRANT SELECT ON audit.v_masked_transactions TO user_dev;
GRANT SELECT ON prod.v_masked_transactions TO user_analyst;

-- Funcion de pseudonimizacion con salting
-- El salt se lee de variable de entorno (no hardcodear en SQL)
-- En produccion, usar: current_setting('app.pii_salt')
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION pseudonymize_user(user_id integer)
RETURNS text AS $$
BEGIN
-- Usa digest con SHA256 y devuelve hex
RETURN encode(digest(CAST(user_id AS text) || 'mi_salt_secreto', 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Esta nueva servirá para emails e IPs
CREATE OR REPLACE FUNCTION pseudonymize_email_ip(input_data text) 
RETURNS text AS $$
BEGIN
    RETURN encode(digest(input_data || 'mi_salt_secreto', 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Vista pseudonimizada para analisis
CREATE OR REPLACE VIEW analytics.v_analytics_transactions AS
SELECT
    t.transaction_id,
    pseudonymize_user(t.user_id) AS user_pseudo_id,
    t.product_category,
    t.amount,
    t.currency,
    t.transaction_date,
    t.status,
    t.data_quality_score
    -- t.transaction_day,
    -- t.transaction_quarter
FROM prod.raw_transactions t;

GRANT SELECT ON analytics.v_analytics_transactions TO user_analyst;

CREATE OR REPLACE VIEW analytics.v_analytics_users AS
SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    pseudonymize_email_ip(u.email) AS pseudo_email,
    pseudonymize_email_ip(u.ip_address) AS pseudo_ip_address,
    u.registration_date,
    u.country,
    u.data_quality_score
FROM prod.raw_users u;

GRANT SELECT ON analytics.v_analytics_users TO user_analyst;

-- TABLA DE CLAVES DE ENCRIPTACION POR USUARIO
-- Cada usuario tiene su propia clave simetrica
CREATE TABLE IF NOT EXISTS audit.user_encryption_keys (
    key_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL UNIQUE,
    encryption_key BYTEA NOT NULL,
    key_version INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    revoked_at TIMESTAMP,
    revoke_reason VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- REGISTRO DE SOLICITUDES DE ELIMINACION (LDPD/GDPR)
CREATE TABLE IF NOT EXISTS audit.data_deletion_requests (
    request_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    request_type VARCHAR(50) CHECK (request_type IN ('deletion', 'access', 'rectification')),
    request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('pending', 'processing', 'completed', 'rejected')),
    completed_at TIMESTAMP,
    processed_by VARCHAR(100),
    notes TEXT,
    legal_reference VARCHAR(200) DEFAULT 'LDPD Art. 12 / GDPR Art. 17'
);

CREATE INDEX IF NOT EXISTS idx_deletion_requests_status
ON audit.data_deletion_requests (status, request_date DESC);



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