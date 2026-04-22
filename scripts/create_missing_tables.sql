CREATE TABLE IF NOT EXISTS audit.user_encryption_keys (
    key_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    encryption_key BYTEA NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    revoked_at TIMESTAMP,
    revoke_reason TEXT
);

CREATE TABLE IF NOT EXISTS audit.data_deletion_requests (
    request_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    request_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    processed_by VARCHAR(100),
    notes TEXT
);
