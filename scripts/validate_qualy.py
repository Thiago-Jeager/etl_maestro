import os
import sys
import argparse
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
load_dotenv()

def validate_table(table_name: str, suite_name: str="transactions_quality_suite"):
    """
    Valida la calidad de datos de una tabla específica usando Great Expectations 0.17.x.
    Carga los datos en un PandasDataset y ejecuta las expectativas del suite JSON.

    Args:
        table_name (str): Nombre completo de la tabla en formato schema.table
        suite_name (str): Nombre del suite de expectativas a utilizar

    Returns:
        bool: True si la validación es exitosa, False en caso contrario
    """
    # Construir la cadena de conexión a PostgreSQL
    connection_string = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}"
        f":{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    # Buscar el archivo de expectativas
    suite_paths = [
        f"/opt/airflow/gx/expectations/{suite_name}.json",
        f"gx/expectations/{suite_name}.json",
    ]
    suite_path = next((p for p in suite_paths if os.path.exists(p)), None)
    if not suite_path:
        print(f"Error: No se encontró el archivo '{suite_name}.json' en las rutas esperadas:")
        for p in suite_paths:
            print(f"  - {p}")
        sys.exit(2)

    with open(suite_path, 'r') as f:
        suite_dict = json.load(f)

    expectations = suite_dict.get('expectations', [])
    if not expectations:
        print(f"Advertencia: El suite '{suite_name}' no contiene expectativas.")
        return True

    # Cargar datos de la tabla
    engine = create_engine(connection_string)
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    if len(df) == 0:
        print(f"Error: La tabla '{table_name}' no contiene datos.")
        return False

    print(f"Validando '{table_name}' con {len(expectations)} expectativas ({len(df)} registros)...")
    print("=" * 60)

    # Crear PandasDataset y ejecutar expectativas
    from great_expectations.dataset.pandas_dataset import PandasDataset
    import inspect

    validator = PandasDataset(df)
    passed, failed, skipped = [], [], []

    for exp in expectations:
        etype = exp.get('expectation_type')
        kwargs = exp.get('kwargs', {}) or {}
        desc = exp.get('meta', {}).get('description', etype)
        sev = exp.get('meta', {}).get('severity', '')
        try:
            method = getattr(validator, etype, None)
            if not method:
                raise AttributeError(f"No soportada en esta versión de GX: {etype}")
            # Filtrar kwargs válidos para la firma del método
            sig = set(inspect.signature(method).parameters.keys())
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig}
            result = method(**filtered_kwargs)

            if result.get('success'):
                label = f"[{sev}] {desc}" if sev else etype
                print(f"  ✅ {label}")
                passed.append(label)
            else:
                uc = result.get('result', {}).get('unexpected_count', 0)
                label = f"[{sev}] {desc}: {uc} registros fallan" if sev else f"{etype}: {uc} fallan"
                print(f"  ❌ {label}")
                failed.append(label)
        except Exception as e:
            label = f"❌ {etype} (error: {e})"
            print(f"  ⚠️  {label}")
            skipped.append(label)

    # Resumen
    total = len(expectations)
    print("=" * 60)
    print(f"Resultados: {len(passed)} pasaron, {len(failed)} fallaron, {len(skipped)} error")
    success_rate = (len(passed) / total * 100) if total > 0 else 0
    print(f"Tasa de éxito: {success_rate:.1f}%")

    if failed:
        print(f"\nExpectativas fallidas:")
        for f in failed:
            print(f"  - {f}")

    if skipped:
        print(f"\nExpectativas con error:")
        for s in skipped:
            print(f"  - {s}")

    return len(failed) == 0 and len(skipped) == 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validar la calidad de datos de una tabla usando Great Expectations.")
    parser.add_argument("--table", required=True, help="Nombre completo de la tabla a validar (schema.table)")
    parser.add_argument("--suite", default="transactions_quality_suite", help="Nombre del suite de expectativas a usar")
    args = parser.parse_args()

    try:
        is_valid = validate_table(args.table, args.suite)
        sys.exit(0 if is_valid else 1)
    except Exception as e:
        print(f"Error durante la validación: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
