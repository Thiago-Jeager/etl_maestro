-- Función de pseudonimización para dbt
CREATE OR REPLACE FUNCTION pseudonymize_user(original_id INT)
RETURNS TEXT AS $$
DECLARE
    salt_value TEXT;
BEGIN
    -- Obtener salt de configuración (seteado por la aplicación)
    salt_value := current_setting('app.pii_salt', TRUE);
    IF salt_value IS NULL OR salt_value = '' THEN
        salt_value := 'DEFAULT_SALT_CHANGE_ME';
    END IF;

    RETURN 'U_' || SUBSTRING(MD5(salt_value || original_id::TEXT) FROM 1 FOR 16);
END;
$$ LANGUAGE plpgsql IMMUTABLE;
