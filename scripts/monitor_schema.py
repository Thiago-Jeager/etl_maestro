import os
import sys
import json
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logger = logging.getLogger(__name__)

DB_URI = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def detect_schema_drift(schema_name: str, table_name: str) -> dict:
    """Compara esquema actual con ultimo registrado."""
    engine = create_engine(DB_URI)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """), {'schema': schema_name, 'table': table_name})
        current = {r[0]: {'type': r[1], 'nullable': r[2]} for r in result}

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT schema_json FROM audit.schema_history
            WHERE schema_name = :s AND table_name = :t
            ORDER BY check_timestamp DESC LIMIT 1
        """), {'s': schema_name, 't': table_name})
        row = result.fetchone()
        previous = json.loads(row[0]) if row and row[0] else None

    changes = {'new_columns': [], 'removed_columns': [], 'type_changes': []}
    drift = False

    if previous:
        new = set(current.keys()) - set(previous.keys())
        if new:
            changes['new_columns'] = list(new)
            drift = True
            logger.warning(f"Nuevas columnas en {schema_name}.{table_name}: {new}")

        removed = set(previous.keys()) - set(current.keys())
        if removed:
            changes['removed_columns'] = list(removed)
            drift = True
            logger.error(f"Columnas removidas: {removed}")

        for col in current:
            if col in previous and current[col]['type'] != previous[col]['type']:
                changes['type_changes'].append({
                    'column': col,
                    'old': previous[col]['type'],
                    'new': current[col]['type']
                })
                drift = True

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO audit.schema_history
            (schema_name, table_name, schema_json, drift_detected, changes_json)
            VALUES (:s, :t, :schema, :drift, :changes)
        """), {
            's': schema_name, 't': table_name,
            'schema': json.dumps(current), 'drift': drift, 'changes': json.dumps(changes)
        })

    return {'drift_detected': drift, 'changes': changes}


if __name__ == '__main__':
    import argparse

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Deteccion de schema drift en tablas del warehouse')
    parser.add_argument('--schema', default='prod', help='Schema de la tabla (default: prod)')
    parser.add_argument('--table', default='raw_transactions', help='Nombre de la tabla')
    args = parser.parse_args()

    print(f"Verificando drift en {args.schema}.{args.table}...")
    result = detect_schema_drift(args.schema, args.table)

    if result['drift_detected']:
        print(f"⚠️  DRIFT DETECTADO:")
        for k, v in result['changes'].items():
            if v:
                print(f"  {k}: {v}")
    else:
        print("✅ Sin cambios en el esquema.")