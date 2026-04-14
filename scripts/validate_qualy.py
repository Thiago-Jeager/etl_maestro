import os
import sys
import argparse
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
load_dotenv()

def validate_table(table_name: str, suite_name: str="transactions_quality_suite"):
    """
    Valida la calidad de datos de una tabla específica usando Great Expectations.

    Args:
        table_name (str): Nombre completo de la tabla en formato schema.table
        suite_name (str): Nombre del suite de expectativas a utilizar

    Returns:
        bool: True si la validación es exitosa, False en caso contrario
    """
    # Construir la cadena de conexión a PostgreSQL usando variables de entorno
    connection_string = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}"
        f":{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    # Inicializar el contexto de Great Expectations
    context = gx.get_context()

    # Agregar datasource PostgreSQL al contexto
    datasource_config = context.sources.add_postgres(
        name="postgres_warehouse",
        connection_string=connection_string
    )

    # Separar schema y tabla del nombre completo
    schema, table = table_name.split(".")

    # Agregar asset de tabla al datasource
    asset = datasource_config.add_table_asset(
        name=f"{schema}_{table}",
        schema_name=schema,
        table_name=table
    )

    # Obtener batch de datos completo de la tabla
    batch_definition = asset.add_batch_definition_whole_table("batch_def")
    batch = batch_definition.get_batch()

    # Intentar obtener el suite de expectativas existente
    try:
        suite = context.get_expectation_suite(suite_name)
    except gx.exceptions.ExpectationSuiteNotFoundError:
        print(f"Error: No se encontró el suite de expectativas '{suite_name}'.")
        # Crear un suite básico si no existe
        suite = ExpectationSuite(expectation_suite_name=suite_name)
        suite.add_expectation(
            gx.expectations.expect_column_values_to_not_be_null(column="transaction_id")
        )

    # Ejecutar la validación
    print(f"Validando la tabla '{table_name}' contra el suite '{suite_name}'...")
    print(":" * 50)

    validation_result = batch.validate(expectation_suite=suite)

    # Procesar e imprimir resultados de validación
    success = validation_result["success"]
    stats = validation_result["statistics"]

    print(f"Validación {'exitosa' if success else 'fallida'}")
    print(f"Total de expectativas: {stats['evaluated_expectations']}")

    if not success:
        for result in validation_result["results"]:
            if not result["success"]:
                print(f"Expectativa fallida: {result['expectation_config']['expectation_type']}")
                print(f"Detalles: {result['result']}")
                if "result" in result and "unexpected_index_list" in result["result"]:
                    print(f"Registros inesperados: {result['result']['unexpected_index_list']}")

    print("Generando reporte de validación...")
    context.build_data_docs()

    return success

if __name__ == "__main__":
    # Configurar parser de argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="Validar la calidad de datos de una tabla usando Great Expectations.")
    parser.add_argument("--table", required=True, help="Nombre completo de la tabla a validar (schema.table)")
    parser.add_argument("--suite", default="transactions_quality_suite", help="Nombre del suite de expectativas a usar para la validación")
    args = parser.parse_args()

    try:
        # Ejecutar validación y obtener resultado
        is_valid = validate_table(args.table, args.suite)
        sys.exit(0 if is_valid else 1)
    except Exception as e:
        print(f"Error durante la validación: {e}")
        sys.exit(2) 