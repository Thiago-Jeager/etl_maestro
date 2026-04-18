# =============================================================================
# dag_maestro_unl.py - Pipeline ETL con patrón WAP y Great Expectations
# Autor: Taller UNL - DataOps Robusto
# Fecha: 2026
# =============================================================================

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import requests
import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURACIÓN - Usar variables de entorno (NO hardcoding)
# =============================================================================
API_KEY = os.getenv('API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://host.docker.internal:5000/api')

DB_CONFIG = {
    'user': os.getenv('DB_USER', 'user_dbt'),
    'password': os.getenv('DB_PASS', 'password_dbt'),
    'host': os.getenv('DB_HOST', 'postgres_warehouse'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'db_warehouse')
}

# Conexión SQLAlchemy para operaciones directas
DB_URI = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# =============================================================================
# FUNCIÓN DE EXTRACCIÓN RESILIENTE (con retry para errores 429/500)
# =============================================================================
def fetch_with_retry(
    url: str,
    headers: Dict,
    params: Dict,
    max_retries: int = 5,
    base_delay: int = 2
) -> Optional[Dict]:
    """
    Realiza request a la API con backoff exponencial para manejar rate limits.

    Args:
        url: Endpoint de la API
        headers: Headers de la request (incluye API key)
        params: Parámetros de query (page, limit)
        max_retries: Número máximo de reintentos
        base_delay: Delay inicial en segundos para backoff

    Returns:
        Dict con la respuesta JSON o None si falla después de todos los intentos
    """
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

    @retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=base_delay, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def _request():
        response = requests.get(url, headers=headers, params=params, timeout=30)

        # Manejar específicamente errores de la API simulada
        if response.status_code == 429:
            logger.warning(f"⚠️ Rate limit (429) - Reintentando en {base_delay}s...")
            raise requests.exceptions.RequestException("Rate limit exceeded")
        elif response.status_code == 500:
            logger.warning(f"⚠️ Server error (500) - Reintentando en {base_delay}s...")
            raise requests.exceptions.RequestException("Internal server error")
        elif response.status_code == 401:
            logger.error("❌ Error de autenticación (401) - No reintentar")
            response.raise_for_status()  # Esto lanzará la excepción y detendrá el retry
        elif response.status_code != 200:
            logger.error(f"❌ Error inesperado {response.status_code}: {response.text}")
            response.raise_for_status()

        return response.json()

    try:
        return _request()
    except Exception as e:
        logger.error(f"❌ Fallo después de {max_retries} intentos: {e}")
        return None


# =============================================================================
# DEFINICIÓN DEL DAG CON TASKFLOW API
# =============================================================================
@dag(
    dag_id='dag_wap_unl_final',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',  # Ejecución diaria
    catchup=False,
    tags=['ETL', 'WAP', 'GreatExpectations', 'UNL'],
    description='Pipeline ETL con patrón WAP para datos de transacciones',
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: logger.error(f"DAG failed: {context}")
    }
)
def taller_etl_unl_wap():

    # -------------------------------------------------------------------------
    # FASE 1: WRITE - Extracción y carga a esquema AUDIT
    # -------------------------------------------------------------------------
    @task(task_id='write_to_audit', retries=3, retry_delay=timedelta(seconds=30))
    def write_to_audit() -> Dict[str, int]:
        """
        Extrae datos de la API y los carga en el esquema audit.raw_transactions.

        Returns:
            Dict con métricas de la extracción
        """
        logger.info("🚀 Iniciando fase WRITE: Extracción de API")

        pg_hook = PostgresHook(postgres_conn_id='postgres_warehouse_conn')

        # Limpiar tabla de auditoría para esta ejecución
        pg_hook.run("TRUNCATE TABLE audit.raw_transactions;")
        logger.info("🗑️ Tabla audit.raw_transactions limpiada")

        # Configurar request
        url = f"{API_BASE_URL}/transactions"
        headers = {"x-api-key": API_KEY}

        # Variables de paginación
        page = 1
        total_pages = 1
        records_inserted = 0
        errors = 0

        # Loop de paginación con manejo de errores
        while page <= total_pages:
            logger.info(f"📄 Solicitando página {page}/{total_pages}")

            params = {"page": page, "limit": 100}
            response_data = fetch_with_retry(url, headers, params)

            if response_data is None:
                errors += 1
                logger.warning(f"⚠️ Fallo al obtener página {page}, continuando...")
                page += 1
                continue

            # Actualizar total de páginas desde metadatos
            meta = response_data.get('meta', {})
            total_pages = meta.get('total_pages', 1)

            # Procesar registros de la página
            for record in response_data.get('data', []):
                try:
                    # Insertar en PostgreSQL usando parámetros para evitar SQL injection
                    sql = """
                        INSERT INTO audit.raw_transactions
                        (transaction_id, user_id, product_category, amount,
                        currency, transaction_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    pg_hook.run(sql, parameters=(
                        record['transaction_id'],
                        record['user_id'],
                        record['product_category'],
                        record['amount'],
                        record['currency'],
                        record['transaction_date'],
                        record['status']
                    ))
                    records_inserted += 1

                except Exception as e:
                    errors += 1
                    logger.error(f"❌ Error insertando registro {record.get('transaction_id')}: {e}")

            page += 1

        # Log de resumen
        logger.info(f"✅ Fase WRITE completada: {records_inserted} registros, {errors} errores")

        return {
            'records_inserted': records_inserted,
            'errors': errors,
            'pages_processed': page - 1
        }

    # -------------------------------------------------------------------------
    # FASE 2: AUDIT - Validación de calidad con Great Expectations
    # -------------------------------------------------------------------------
    @task(task_id='audit_with_gx')
    def audit_with_gx(extract_metrics: Dict[str, int]) -> bool:
        """
        Ejecuta validaciones de calidad usando Great Expectations.
        Carga dos suites: CRITICAL (obligatorias) y WARNINGS (alertas).
        Registra anomalías pero permite continuar el pipeline (patrón WAP tolerante).

        Args:
            extract_metrics: Métricas de la fase de extracción

        Returns:
            True si puede continuar el pipeline
        """
        logger.info("🔍 Iniciando fase AUDIT: Validación con Great Expectations")

        # Conexión a la base de datos
        engine = create_engine(DB_URI)

        # -------------------------------------------------------------------------
        # VALIDACIÓN 1: Chequeo rápido con SQL para fallos obvios
        # -------------------------------------------------------------------------
        logger.info("📋 Ejecutando validaciones SQL preliminares...")

        # Detectar montos negativos (la API inyecta 4% intencionalmente)
        # En patrón WAP: detectar pero NO bloquear - dejar que PUBLISH filtre
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM audit.raw_transactions WHERE amount < 0"
            ))
            negative_count = result.scalar()

        if negative_count > 0:
            logger.warning(f"⚠️ ADVERTENCIA: {negative_count} registros con montos negativos detectados")
            logger.warning(f"  Estos serán filtrados en la fase PUBLISH. La validación continúa...")

        # Detectar valores nulos en campos obligatorios
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_users,
                    COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) as null_ids
                FROM audit.raw_transactions
            """))
            null_stats = result.fetchone()

        if null_stats.null_users > 0 or null_stats.null_ids > 0:
            logger.warning(f"⚠️ Advertencia: {null_stats.null_users} user_ids nulos, "
                           f"{null_stats.null_ids} transaction_ids nulos")

        # -------------------------------------------------------------------------
        # VALIDACIÓN 2: Great Expectations (suites CRITICAL y WARNINGS)
        # -------------------------------------------------------------------------
        logger.info("🧪 Ejecutando suites de Great Expectations...")

        try:
            import great_expectations as gx
            import json
            import pandas as pd
            import os
            import inspect
            from great_expectations.dataset.pandas_dataset import PandasDataset

            # Definir rutas de búsqueda para los archivos de suite
            suite_files = {
                'critical': [
                    "/opt/airflow/gx/expectations/transactions_critical_suite.json",
                    os.path.join(os.getcwd(), "gx/expectations/transactions_critical_suite.json"),
                    "gx/expectations/transactions_critical_suite.json",
                    "./gx/expectations/transactions_critical_suite.json"
                ],
                'warnings': [
                    "/opt/airflow/gx/expectations/transactions_warnings_suite.json",
                    os.path.join(os.getcwd(), "gx/expectations/transactions_warnings_suite.json"),
                    "gx/expectations/transactions_warnings_suite.json",
                    "./gx/expectations/transactions_warnings_suite.json"
                ]
            }

            # Cargar datos una sola vez (para ambas suites)
            logger.info("📊 Leyendo datos de audit.raw_transactions para validar...")
            query = "SELECT * FROM audit.raw_transactions"
            df = pd.read_sql(query, con=engine)
            total_records = len(df)
            logger.info(f"📊 Total de registros a validar: {total_records}")

            if total_records == 0:
                logger.warning("⚠️ No hay registros en audit.raw_transactions para validar")
                return True

            # Crear validador una sola vez
            validator = PandasDataset(df)
            logger.info("✅ Validador GX creado con PandasDataset")

            # Estructura para almacenar resultados
            overall_results = {
                'critical': {'passed': 0, 'failed': 0, 'details': []},
                'warnings': {'passed': 0, 'failed': 0, 'details': []}
            }

            # Función auxiliar para ejecutar una suite
            def execute_suite(suite_type: str, suite_paths: List[str]) -> None:
                """Ejecuta las expectativas de una suite (CRITICAL o WARNINGS)"""
                
                # Buscar archivo de suite
                suite_path = None
                for path in suite_paths:
                    if os.path.exists(path):
                        suite_path = path
                        logger.info(f"📂 Suite {suite_type} encontrada en: {path}")
                        break

                if suite_path is None:
                    logger.warning(f"⚠️ No se encontró suite {suite_type} en las rutas esperadas")
                    return

                # Cargar suite desde JSON
                try:
                    with open(suite_path, 'r') as f:
                        suite_dict = json.load(f)
                except (FileNotFoundError, IOError, json.JSONDecodeError) as io_err:
                    logger.warning(f"⚠️ Error leyendo suite {suite_type}: {str(io_err)[:100]}")
                    return

                suite_name = suite_dict.get('expectation_suite_name', f'transactions_{suite_type}_suite')
                expectations = suite_dict.get('expectations', []) or []
                logger.info(f"\n{'='*60}")
                logger.info(f"🔍 Ejecutando suite {suite_type.upper()}: '{suite_name}'")
                logger.info(f"   Total de expectativas: {len(expectations)}")
                logger.info(f"{'='*60}")

                # Ejecutar cada expectativa
                for idx, expectation in enumerate(expectations, 1):
                    expectation_type = expectation.get('expectation_type')
                    kwargs = expectation.get('kwargs', {}) or {}
                    meta = expectation.get('meta', {})
                    description = meta.get('description', expectation_type)
                    severity = meta.get('severity', 'INFO')

                    try:
                        if not expectation_type:
                            raise ValueError("Expectation sin expectation_type")

                        # Obtener el método del validador
                        method = getattr(validator, expectation_type, None)
                        if method is None:
                            raise AttributeError(f"Expectation no soportada: {expectation_type}")

                        # Filtrar parámetros soportados por el método
                        supported_params = set(inspect.signature(method).parameters.keys())
                        supported_kwargs = {k: v for k, v in kwargs.items() if k in supported_params}

                        # Ejecutar expectativa
                        result = method(**supported_kwargs)
                        success = result.get('success', False)
                        unexpected_count = result.get('result', {}).get('unexpected_count', 0)

                        if success:
                            logger.info(f"  [{idx}] ✅ {description}")
                            overall_results[suite_type]['passed'] += 1
                        else:
                            icon = "⛔" if severity == "CRITICAL" else "⚠️"
                            logger.warning(f"  [{idx}] {icon} {description} [{severity}]")
                            logger.warning(f"       └─ {unexpected_count} registros no cumplen")
                            overall_results[suite_type]['failed'] += 1
                            overall_results[suite_type]['details'].append({
                                'expectation': expectation_type,
                                'description': description,
                                'severity': severity,
                                'unexpected_count': unexpected_count
                            })

                    except Exception as exp_error:
                        logger.warning(f"  [{idx}] ❌ Error en {expectation_type}: {str(exp_error)[:80]}")
                        overall_results[suite_type]['failed'] += 1

                # Resumen de suite
                passed = overall_results[suite_type]['passed']
                failed = overall_results[suite_type]['failed']
                total = passed + failed
                success_rate = (passed / total * 100) if total > 0 else 0

                logger.info(f"\n{'─'*60}")
                logger.info(f"📊 RESUMEN SUITE {suite_type.upper()}")
                logger.info(f"  ✅ Pasadas:  {passed}/{total}")
                logger.info(f"  ❌ Fallidas: {failed}/{total}")
                logger.info(f"  📈 Éxito:    {success_rate:.1f}%")
                logger.info(f"{'─'*60}\n")

            # Ejecutar ambas suites
            execute_suite('critical', suite_files['critical'])
            execute_suite('warnings', suite_files['warnings'])

            # Registrar resultados en tabla de logs
            total_passed = overall_results['critical']['passed'] + overall_results['warnings']['passed']
            total_failed = overall_results['critical']['failed'] + overall_results['warnings']['failed']
            total_expectations = total_passed + total_failed

            if total_expectations > 0:
                overall_success_rate = (total_passed / total_expectations * 100)
            else:
                overall_success_rate = 0

            try:
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO audit.gx_validation_logs
                        (table_name, expectation_suite_name, total_records,
                        failed_records, success_rate, critical_failures)
                        VALUES (:table_name, :expectation_suite_name, :total_records,
                        :failed_records, :success_rate, :critical_failures)
                    """), {
                        "table_name": "audit.raw_transactions",
                        "expectation_suite_name": "transactions_critical_suite + transactions_warnings_suite",
                        "total_records": total_expectations,
                        "failed_records": total_failed,
                        "success_rate": overall_success_rate,
                        "critical_failures": [json.dumps(item) for item in overall_results['critical']['details']]
                    })
            except Exception as db_err:
                logger.warning(f"⚠️ No se pudo registrar en gx_validation_logs: {str(db_err)[:100]}")

            # Resumen final
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 RESUMEN FINAL DE VALIDACIÓN")
            logger.info(f"{'='*60}")
            logger.info(f"  CRITICAL - ✅: {overall_results['critical']['passed']} | ❌: {overall_results['critical']['failed']}")
            logger.info(f"  WARNINGS - ✅: {overall_results['warnings']['passed']} | ❌: {overall_results['warnings']['failed']}")
            logger.info(f"  ─────────────────────────────────────")
            logger.info(f"  TOTAL    - ✅: {total_passed} | ❌: {total_failed}")
            logger.info(f"  Tasa de éxito: {overall_success_rate:.1f}%")
            logger.info(f"{'='*60}")

            if overall_results['critical']['failed'] > 0:
                logger.warning(f"⚠️ {overall_results['critical']['failed']} validaciones CRITICAL fallaron")
                logger.warning(f"   Pero el pipeline continúa (patrón WAP tolerante)")

            logger.info("✅ Fase AUDIT completada - Pipeline puede continuar")
            return True

        except ImportError as ie:
            logger.warning(f"⚠️ Error importando Great Expectations: {str(ie)[:100]}")
            logger.warning("⚠️ Continuando sin validaciones avanzadas...")
            return True
        except Exception as e:
            logger.error(f"❌ Error en validación GX: {str(e)[:200]}")
            logger.warning("⚠️ Continuando pipeline a pesar del error")
            return True

    # -------------------------------------------------------------------------
    # FASE 3: PUBLISH - Transformación y carga a esquema PROD con Spark
    # -------------------------------------------------------------------------
    @task(task_id='publish_with_spark')
    def publish_with_spark(audit_passed: bool) -> Dict[str, int]:
        """
        Usa Spark para transformar datos y cargarlos al esquema prod.
        Solo se ejecuta si la auditoría fue exitosa.

        Args:
            audit_passed: Resultado de la fase de auditoría

        Returns:
            Dict con métricas de la publicación
        """
        if audit_passed is None:
            logger.warning("⚠️ No se recibió el resultado de auditoría. Si ejecutas esta tarea aisladamente, el DAG completo no fue disparado.")
            logger.warning("⚠️ Saltando fase PUBLISH para evitar publicar sin una auditoría previa.")
            return {'records_published': 0, 'reason': 'audit_result_missing'}

        if not audit_passed:
            logger.warning("⚠️ Auditoría fallida, saltando fase PUBLISH")
            return {'records_published': 0, 'reason': 'audit_failed'}

        logger.info("🚀 Iniciando fase PUBLISH: Transformación con Spark")

        # Configuración de Spark
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, when, lit, current_timestamp
        except ImportError as ie:
            logger.error("❌ pyspark no está instalado en el entorno de Airflow. Reconstruye la imagen Docker y reinicia los servicios.")
            raise

        spark = SparkSession.builder \
            .appName("ETL_Publish_WAP") \
            .master("local[*]") \
            .config("spark.driver.extraClassPath", "/opt/spark-jars/postgresql-42.5.0.jar") \
            .config("spark.executor.extraClassPath", "/opt/spark-jars/postgresql-42.5.0.jar") \
            .getOrCreate()

        try:
            # Propiedades de conexión JDBC
            jdbc_properties = {
                "user": DB_CONFIG['user'],
                "password": DB_CONFIG['password'],
                "driver": "org.postgresql.Driver"
            }
            jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

            # Leer datos del esquema audit
            logger.info("📥 Leyendo datos desde audit.raw_transactions")
            df_audit = spark.read.jdbc(
                url=jdbc_url,
                table="audit.raw_transactions",
                properties=jdbc_properties
            )

            total_records = df_audit.count()
            logger.info(f"📊 Registros en audit: {total_records}")

            # -------------------------------------------------------------------------
            # TRANSFORMACIONES DE CALIDAD
            # -------------------------------------------------------------------------
            logger.info("🔧 Aplicando transformaciones de limpieza...")

            # 1. Filtrar transacciones completadas y con monto válido
            df_clean = df_audit.filter(
                (col("status") == "COMPLETED") &
                (col("amount") > 0) &
                (col("user_id").isNotNull())
            )

            # 2. Añadir columnas de enriquecimiento
            df_enriched = df_clean.withColumn(
                "data_quality_score",
                lit(1.0)  # En producción: calcular score basado en reglas
            ).withColumn(
                "processed_at",
                current_timestamp()
            )

            # 3. Ofuscar PII si fuera necesario (ejemplo: hash de user_id)
            # from pyspark.sql.functions import sha2
            # df_enriched = df_enriched.withColumn(
            #     "user_id_hashed",
            #     sha2(col("user_id").cast("string"), 256)
            # )

            # -------------------------------------------------------------------------
            # CARGA A PRODUCCIÓN
            # -------------------------------------------------------------------------
            logger.info("📤 Preparando datos nuevos para prod.raw_transactions")

            df_prod_existing = spark.read.jdbc(
                url=jdbc_url,
                table="prod.raw_transactions",
                properties=jdbc_properties
            ).select("transaction_id")

            df_to_publish = df_enriched.dropDuplicates(["transaction_id"]).join(
                df_prod_existing,
                on="transaction_id",
                how="left_anti"
            )

            new_records = df_to_publish.count()
            duplicate_records = df_enriched.dropDuplicates(["transaction_id"]).count() - new_records

            if new_records == 0:
                logger.info("⚠️ No hay registros nuevos para publicar en prod.raw_transactions.")
                return {
                    'records_published': 0,
                    'records_filtered': total_records,
                    'duplicate_records': duplicate_records,
                    'approval_rate': 0
                }

            logger.info(f"📤 Publicando {new_records} registros nuevos en prod.raw_transactions")

            df_to_publish.write.jdbc(
                url=jdbc_url,
                table="prod.raw_transactions",
                mode="append",
                properties=jdbc_properties
            )

            published_count = new_records
            filtered_count = total_records - published_count

            logger.info(f"✅ Fase PUBLISH completada:")
            logger.info(f"   • Registros publicados: {published_count}")
            logger.info(f"   • Registros filtrados: {filtered_count}")
            logger.info(f"   • Registros duplicados evitados: {duplicate_records}")
            logger.info(f"   • Tasa de aprobación: {published_count/total_records*100:.1f}%")

            return {
                'records_published': published_count,
                'records_filtered': filtered_count,
                'duplicate_records': duplicate_records,
                'approval_rate': round(published_count/total_records*100, 2) if total_records > 0 else 0
            }

        finally:
            spark.stop()
            logger.info("🛑 Sesión de Spark cerrada")

    # -------------------------------------------------------------------------
    # FASE 4: DBT - Materialización de modelos analíticos
    # -------------------------------------------------------------------------
    @task(task_id='materialize_with_dbt')
    def materialize_with_dbt(publish_metrics: Dict[str, int]) -> bool:
        """
        Ejecuta dbt run para materializar modelos analíticos finales.

        Args:
            publish_metrics: Métricas de la fase de publicación

        Returns:
            True si dbt se ejecutó exitosamente
        """
        import subprocess

        logger.info("🔄 Iniciando fase DBT: Materialización de modelos")

        dbt_dir = "/opt/airflow/dbt/proyecto_unl"
        dbt_profiles_dir = "/opt/airflow/dbt"

        # Comando dbt seguro (sin shell=True)
        cmd = ["dbt", "run", "--profiles-dir", dbt_profiles_dir, "--target", "prod"]

        try:
            result = subprocess.run(
                cmd,
                cwd=dbt_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutos máximo
            )

            if result.returncode != 0:
                logger.error("❌ Error ejecutando dbt run:")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                raise AirflowFailException(f"dbt run falló con código {result.returncode}")

            logger.info("✅ dbt run completado exitosamente")
            logger.info(f"📄 Output: {result.stdout[-500:]}")  # Últimos 500 caracteres

            return True

        except subprocess.TimeoutExpired:
            logger.error("⏰ dbt run excedió el tiempo límite de 5 minutos")
            raise
        except FileNotFoundError:
            logger.error("❌ Comando 'dbt' no encontrado. Verificar instalación en Dockerfile")
            raise

    # -------------------------------------------------------------------------
    # DEFINIR FLUJO DE TAREAS (DAG)
    # -------------------------------------------------------------------------

    # Ejecutar fases en secuencia con paso de métricas entre tareas
    extract_result = write_to_audit()
    audit_result = audit_with_gx(extract_result)
    publish_result = publish_with_spark(audit_result)
    dbt_result = materialize_with_dbt(publish_result)

    # El DAG retorna el resultado final para logging
    return {"status": "completed", "timestamp": datetime.now().isoformat()}


# Instanciar el DAG para que Airflow lo registre
dag_instance = taller_etl_unl_wap()
