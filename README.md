# 🏗️ ARQUITECTURA DEL PROYECTO ETL - Taller UNL

## 📋 Índice
1. [Visión General](#visión-general)
2. [Componentes Principales](#componentes-principales)
3. [Patrones de Diseño](#patrones-de-diseño)
4. [Flujo de Datos](#flujo-de-datos)
5. [Stack Tecnológico](#stack-tecnológico)
6. [Estructura de Directorios](#estructura-de-directorios)
7. [Seguridad y Privacidad de Datos](#seguridad-y-privacidad-de-datos)
8. [Aseguramiento de Calidad](#aseguramiento-de-calidad)

---

## 🎯 Visión General

Este proyecto implementa un **Pipeline ETL completo y robusto** siguiendo el patrón **WAP (Write-Audit-Publish)** con énfasis en:

- ✅ **Resiliencia**: Manejo de errores, reintentos con backoff exponencial
- ✅ **Calidad de Datos**: Validaciones con Great Expectations
- ✅ **Privacidad**: Enmascaramiento de PII, hash SHA256
- ✅ **Orquestación**: Apache Airflow con DAGs modernos
- ✅ **Transformaciones**: dbt para modelado de datos
- ✅ **Infraestructura**: Contenedorización con Docker

---

## 🔧 Componentes Principales

### 1. **Apache Airflow 2.8.1**
**Rol**: Orquestación y programación de flujos de trabajo

```yaml
Componentes:
  - Webserver (http://localhost:8080)
    └─ UI interactiva para monitorear DAGs
  
  - Scheduler
    └─ Ejecuta tareas según cronograma (@daily)
  
  - LocalExecutor
    └─ Ejecutor local de tareas (en desarrollo)
  
  - PostgreSQL (Airflow metastore)
    └─ Almacena DAGs, conexiones, logs
```

**Características**:
- TaskFlow API (decoradores @task, @dag)
- Retry automático (2 reintentos, 5min delay)
- Logging estructurado
- Manejo de variables de entorno via .env

---

### 2. **PostgreSQL 13**
**Rol**: Base de datos relacional principal

```sql
Esquemas:
├── audit
│   ├─ raw_transactions (datos crudos de API)
│   ├─ raw_users (datos de usuarios con PII)
│   ├─ validation_results (resultados GX)
│   └─ masked_transactions (transacciones enmascaradas)
│
├── public
│   ├─ stg_* (tablas staging de dbt)
│   ├─ int_* (tablas intermedias)
│   ├─ fct_* (tablas de hechos)
│   ├─ dim_* (tablas de dimensiones)
│   └─ analytics_* (vistas analíticas)
└─ Usuarios especializados:
    ├─ user_dbt (credenciales de transformación)
    └─ airflow (metastore de Airflow)
```

---

### 3. **Apache Spark 3.3.0**
**Rol**: Procesamiento distribuido de datos a escala

```python
Funcionalidades:
├─ Lectura de datos desde PostgreSQL
├─ Filtrado y limpieza de datos
├─ Deduplicación
├─ Enmascaramiento de PII
├─ Escritura en esquema prod
└─ Soporte JDBC PostgreSQL
```

**Características**:
- Driver JDBC: postgresql-42.5.0.jar
- Integración con Airflow vía PySpark
- DataFrame operations optimizadas

---

### 4. **Great Expectations 0.17.15**
**Rol**: Validación y aseguramiento de calidad de datos

```yaml
Estructura:
gx/
├── great_expectations.yml (configuración)
├── expectations/
│   ├─ transactions_critical_suite.json (bloqueante)
│   ├─ transactions_warnings_suite.json (log)
│   ├─ users_critical_suite.json (bloqueante)
│   └─ users_warnings_suite.json (log)
├── checkpoints/ (definiciones de validación)
├── plugins/
│   └─ custom_data_docs/ (documentación personalizada)
└── uncommitted/
    └─ data_docs/ (reportes de validación)
```

**Validaciones Críticas (Bloquean el flujo)**:
- Unicidad de claves primarias
- Integridad referencial
- Valores no nulos en campos obligatorios
- Formato y rango de datos

**Validaciones de Advertencia (Log)**:
- Valores outliers
- Nulos esperados en ciertos campos (ej: 5% en email)
- Formato de email
- Formato de IP (IPv4/IPv6)

---

### 5. **dbt (Data Build Tool) 1.5.0**
**Rol**: Transformación y modelado de datos

```yaml
Proyecto: proyecto_unl
├─ Models (3 capas):
│  ├─ Staging (stg_*)
│  │  └─ Normalización y limpieza de datos crudos
│  ├─ Intermediate (int_*)
│  │  └─ Enriquecimiento (RFM, agregaciones)
│  └─ Marts (fct_*, dim_*)
│     └─ Tablas analíticas listas para BI
│
├─ Macros (reutilizables)
├─ Sources (definición de tablas origen)
└─ Tests (validaciones en SQL)
```

**Layers**:
- **Staging**: stg_raw_transactions, stg_raw_users, stg_masked_transactions
- **Intermediate**: int_transactions_enriched, int_users_enriched
- **Marts**: fct_transactions, fct_users, dim_users, analytics_transactions, analytics_users

---

### 6. **Docker & Docker Compose**
**Rol**: Containerización y orquestación de servicios

```yaml
Servicios:
├── postgres (Airflow metastore)
├── postgres_warehouse (BD principal)
├── airflow-webserver (UI)
├── airflow-scheduler (Orquestación)
└── airflow-init (Configuración inicial)
```

**Volúmenes**:
- `./dags` → `/opt/airflow/dags`
- `./logs` → `/opt/airflow/logs`
- `./dbt` → `/opt/airflow/dbt`
- `./gx` → `/opt/airflow/gx`
- `./scripts` → `/opt/airflow/scripts`
- `./data_lake` → `/opt/airflow/data_lake`

---

## 📊 Patrones de Diseño

### Patrón WAP (Write-Audit-Publish)

```
┌─────────────────────────────────────────────────────────────┐
│                      WRITE PHASE                            │
├─────────────────────────────────────────────────────────────┤
│  1. Extracción de APIs externas (transacciones, usuarios)   │
│  2. Carga sin transformación en esquema AUDIT               │
│  3. Preservación de datos originales (PII intacto)          │
│  4. Manejo de rate limits y reintentos                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    AUDIT PHASE                              │
├─────────────────────────────────────────────────────────────┤
│  1. Validación con Great Expectations                       │
│  2. Críticas: bloquean el flujo                             │
│  3. Advertencias: continúan pero registran                  │
│  4. Generación de data docs (reportes)                      │
│  5. Métricas de calidad (data_quality_score)                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  PUBLISH PHASE                              │
├─────────────────────────────────────────────────────────────┤
│  1. Filtrado de datos válidos                               │
│  2. Enmascaramiento de PII (Spark)                          │
│  3. Deduplicación                                           │
│  4. Carga en esquema PROD                                   │
│  5. Datos listos para dbt                                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   DBT PIPELINE                              │
├─────────────────────────────────────────────────────────────┤
│  1. Staging: Normalización de datos prod                    │
│  2. Intermediate: Enriquecimiento y agregaciones            │
│  3. Marts: Tablas analíticas finales                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 Flujo de Datos Detallado

### 1️⃣ WRITE: Extracción de Datos

```python
Función: write_to_audit()

Proceso:
├─ Conectar a API_BASE_URL/transactions
├─ Paginación (100 registros/página)
├─ Retry con backoff exponencial (429, 500)
├─ Truncate audit.raw_transactions
├─ Insert datos crudos en AUDIT
└─ Retorna métrica: {records_inserted, errors}
```

**Campos de Transacciones**:
- `transaction_id` (PK)
- `user_id` (FK)
- `amount` (16,2 monetario)
- `currency`
- `status` (COMPLETED, PENDING, FAILED)
- 12 campos monetarios adicionales
- `gx_validation_status`
- `ingested_at` (timestamp)

**Campos de Usuarios**:
- `user_id` (PK)
- `first_name`, `last_name`
- `email` (5% intencionales nulos)
- `ip_address` (IPv4/IPv6)
- `country`
- `registration_date`

---

### 2️⃣ AUDIT: Validación con Great Expectations

```python
Función: audit_with_gx()

Validaciones Críticas (bloquean):
├─ PK unique (transaction_id)
├─ FK no nulo (user_id)
├─ amount > 0
├─ status en {COMPLETED, PENDING, FAILED, REFUNDED}
├─ currency en lista válida
└─ ingested_at no nulo

Validaciones de Advertencia (log):
├─ NULL outliers en valores
├─ Montos fuera de rango típico
└─ Duplicados parciales
```

---

### 3️⃣ PUBLISH: Enmascaramiento y Transformación

```python
Función: publish_with_spark()

Spark Operations:
├─ Filter: status = 'COMPLETED' AND amount > 0
├─ Apply Masking Rules
│  ├─ transaction_id: últimos 4 dígitos visibles
│  ├─ email: últimos 4 caracteres visibles
│  └─ ip_address: últimos 3 octetos visibles
├─ Dedup por (transaction_id, user_id)
├─ Append a prod.raw_transactions
└─ Set data_quality_score = 1.0
```

**Reglas de Enmascaramiento** (config/masking_rules.json):
```json
{
  "transaction_id": "***1234",
  "email": "user****@domain.com",
  "ip_address": "192.168.***.***"
}
```

---

### 4️⃣ DBT: Transformaciones

#### Staging Layer (stg_*)
```sql
-- stg_raw_transactions
SELECT
  transaction_id,
  user_id,
  amount,
  currency,
  status,
  ingested_at as source_timestamp
FROM prod.raw_transactions
WHERE data_quality_score >= 0.9

-- stg_masked_users
SELECT
  user_id,
  first_name,
  last_name,
  email_hashed,  -- SHA256
  ip_address_hashed,
  country
FROM prod.raw_users
```

#### Intermediate Layer (int_*)
```sql
-- int_transactions_enriched
SELECT
  t.transaction_id,
  t.user_id,
  t.amount,
  t.currency,
  u.country as user_country,
  ROW_NUMBER() OVER (PARTITION BY t.user_id ORDER BY t.ingested_at) as txn_seq
FROM stg_raw_transactions t
LEFT JOIN stg_masked_users u ON t.user_id = u.user_id

-- int_users_enriched (RFM Analysis)
SELECT
  user_id,
  COUNT(DISTINCT transaction_id) as transaction_count,
  SUM(amount) as total_spent,
  MAX(ingested_at) as last_transaction_date
FROM stg_raw_transactions
GROUP BY user_id
```

#### Marts Layer (fct_* & dim_*)
```sql
-- fct_transactions
SELECT * FROM int_transactions_enriched
-- Materialized table para queries BI

-- dim_users
SELECT
  u.user_id,
  u.first_name,
  u.last_name,
  CASE
    WHEN rfm.total_spent > 5000 THEN 'VIP'
    WHEN rfm.total_spent > 1000 THEN 'Premium'
    ELSE 'Standard'
  END as segment
FROM stg_masked_users u
LEFT JOIN int_users_enriched rfm ON u.user_id = rfm.user_id
```

---

## 💻 Stack Tecnológico

| Componente | Versión | Propósito |
|-----------|---------|----------|
| **Apache Airflow** | 2.8.1 | Orquestación de DAGs |
| **Apache Spark** | 3.3.0 | Procesamiento distribuido |
| **PostgreSQL** | 13 | Base de datos relacional |
| **dbt** | 1.5.0 | Transformaciones SQL |
| **Great Expectations** | 0.17.15 | Validación de calidad |
| **Python** | 3.10+ | Lenguaje principal |
| **Docker** | Latest | Containerización |
| **PySpark** | 3.3.0 | Spark con Python |
| **Pandas** | 1.5.2 | Manipulación de datos |
| **SQLAlchemy** | 1.4.39 | ORM Python |
| **Requests** | 2.28.1 | Cliente HTTP |
| **Tenacity** | 8.0.1 | Retry automático |
| **Cryptography** | 41.0.0+ | Enmascaramiento/Hash |

---

## 📁 Estructura de Directorios

```
taller_etl_master/
│
├── 🔴 RAÍZ
│   ├── docker-compose.yaml          # Orquestación de servicios
│   ├── Dockerfile                   # Imagen de Airflow customizada
│   ├── requirements.txt              # Dependencias Python
│   ├── ARCHITECTURE_USERS.md         # Documentación (usuarios)
│   └── ARQUITECTURA_PROYECTO.md     # Este archivo
│
├── 📅 dags/
│   ├── dag_maestro_etl.py           # DAG principal (WRITE-AUDIT-PUBLISH)
│   └── __pycache__/
│
├── 🗄️ dbt/
│   ├── profiles.yml                 # Configuración dbt (host, credenciales)
│   ├── proyecto_unl/
│   │   ├── dbt_project.yml          # Configuración del proyecto
│   │   ├── packages.yml             # Dependencias dbt
│   │   ├── models/
│   │   │   ├── schema.yml           # Definición de esquemas
│   │   │   ├── sources.yml          # Definición de fuentes
│   │   │   ├── staging/
│   │   │   │   ├── stg_raw_transactions.sql
│   │   │   │   ├── stg_raw_users.sql
│   │   │   │   ├── stg_masked_transactions.sql
│   │   │   │   └── stg_masked_users.sql
│   │   │   ├── intermediate/
│   │   │   │   ├── int_transactions_enriched.sql
│   │   │   │   └── int_users_enriched.sql
│   │   │   └── marts/
│   │   │       ├── fct_transactions.sql
│   │   │       ├── fct_users.sql
│   │   │       ├── dim_users.sql
│   │   │       ├── analytics_transactions.sql
│   │   │       └── analytics_users.sql
│   │   ├── macros/                  # Macros SQL reutilizables
│   │   ├── tests/                   # Tests dbt
│   │   ├── logs/                    # Logs de ejecución
│   │   ├── dbt_packages/            # Paquetes instalados
│   │   └── target/                  # Artefactos compilados
│   └── dbt_docs/
│       ├── index.html               # Documentación HTML
│       └── catalog.json             # Metadatos de catálogo
│
├── ✅ gx/ (Great Expectations)
│   ├── great_expectations.yml       # Configuración GX
│   ├── checkpoints/                 # Definiciones de validación
│   ├── expectations/
│   │   ├── transactions_critical_suite.json
│   │   ├── transactions_warnings_suite.json
│   │   ├── users_critical_suite.json
│   │   └── users_warnings_suite.json
│   ├── plugins/
│   │   └── custom_data_docs/        # Documentación personalizada
│   ├── profilers/                   # Perfiles de datos
│   └── uncommitted/
│       ├── config_variables.yml     # Variables de configuración
│       └── data_docs/               # Reportes de validación
│
├── 💾 data_lake/
│   ├── raw/                         # Datos crudos (CSV, JSON)
│   └── silver/                      # Datos procesados (Parquet)
│
├── 🔧 scripts/
│   ├── masking_transform.py         # Funciones de enmascaramiento
│   ├── crypto_shredding.py          # Eliminación segura de PII
│   ├── masking_transform.py         # Transformación de datos
│   ├── monitor_schema.py            # Monitoreo de esquema
│   ├── validate_qualy.py            # Validación de calidad
│   ├── create_missing_tables.sql    # Inicialización de tablas
│   ├── create_pseudonymize_function.sql # Función de seudonimización
│   └── __pycache__/
│
├── ⚙️ config/
│   └── masking_rules.json           # Reglas de enmascaramiento
│
├── 🗃️ init_db/
│   └── setup_wap.sql                # Script de inicialización BD
│
├── 📊 dbt_docs/
│   ├── catalog.json
│   └── index.html
│
└── 📝 logs/
    ├── dag_id=dag_maestro_etl/
    ├── dag_id=dag_wap_unl_final/
    └── scheduler/
```

---

## 🔐 Seguridad y Privacidad de Datos

### Enmascaramiento de PII

**Campos Protegidos**:
1. **email** → SHA256 hash + máscara
2. **ip_address** → SHA256 hash + máscara
3. **transaction_id** → Máscara (últimos 4 visibles)

**Niveles de Protección**:

| Esquema | PII Visible | PII Hash | Notas |
|---------|-------------|----------|-------|
| **audit** | ✅ Sí | ❌ No | Raw data, acceso limitado |
| **prod** | ❌ No | ✅ SHA256 | Datos publicados |
| **public** | ❌ No | ✅ SHA256 | Listas para analítica |

### Funciones de Seguridad

```python
# masking_transform.py
apply_masking()           # Enmascaramiento Spark
apply_masking_users()     # Enmascaramiento específico usuarios
load_masking_rules()      # Carga de reglas JSON
```

```sql
-- Scripts de BD
create_pseudonymize_function.sql   # Seudonimización SQL
crypto_shredding.py                # Eliminación segura de datos sensibles
```

---

## ✅ Aseguramiento de Calidad

### Great Expectations Suites

#### Transacciones - Críticas (Bloquean)
```json
{
  "suite_name": "transactions_critical_suite",
  "expectations": [
    "expect_table_row_count_to_be_between",
    "expect_column_values_to_be_unique (transaction_id)",
    "expect_column_values_to_not_be_null (user_id)",
    "expect_column_values_to_be_in_set (status)",
    "expect_column_values_to_be_of_type (amount, NUMERIC)"
  ]
}
```

#### Transacciones - Advertencias (Log)
```json
{
  "suite_name": "transactions_warnings_suite",
  "expectations": [
    "expect_column_values_to_be_between (amount, 0, 100000)",
    "expect_column_quantile_values_to_be_between"
  ]
}
```

#### Usuarios - Críticas (Bloquean)
```json
{
  "suite_name": "users_critical_suite",
  "expectations": [
    "expect_column_values_to_be_unique (user_id)",
    "expect_column_values_to_not_be_null (first_name, last_name)",
    "expect_column_values_to_not_be_null (ip_address)",
    "expect_column_values_to_match_regex (ip_address, IPv4|IPv6)"
  ]
}
```

#### Usuarios - Advertencias (Log)
```json
{
  "suite_name": "users_warnings_suite",
  "expectations": [
    "expect_column_values_to_be_null (email, tolerance=0.05)",
    "expect_column_values_to_match_regex (email, EMAIL_REGEX)"
  ]
}
```

### Métricas de Calidad

```sql
-- En cada tabla staging/marts
SELECT
  COUNT(*) as total_rows,
  COUNT(CASE WHEN id IS NULL THEN 1 END) / COUNT(*) as null_rate,
  COUNT(DISTINCT id) / COUNT(*) as uniqueness_rate,
  MIN(created_at) as earliest_record,
  MAX(created_at) as latest_record
FROM table_name
```

---

## 🚀 Flujo de Ejecución Diario

```
00:00 ├─ Scheduler dispara dag_wap_unl_final
      │
      ├─ [WRITE] write_to_audit()
      │   └─ Extrae API_BASE_URL/transactions
      │   └─ Carga audit.raw_transactions
      │   └─ Retorna métricas
      │
      ├─ [WRITE] write_users_to_audit() (NUEVO)
      │   └─ Extrae API_BASE_URL/users
      │   └─ Carga audit.raw_users (con PII)
      │   └─ Retorna métricas
      │
      ├─ [AUDIT] audit_with_gx()
      │   ├─ Valida transactions_critical_suite
      │   │   └─ Falla: continúa con warnings (LOG)
      │   ├─ Valida transactions_warnings_suite
      │   │   └─ Log de problemas
      │   └─ Valida users_critical_suite
      │       └─ Falla: detiene ejecución si bloqueante
      │
      ├─ [PUBLISH] publish_with_spark()
      │   ├─ Filter: status = COMPLETED, amount > 0
      │   ├─ apply_masking() para transacciones
      │   ├─ Dedup + append a prod.raw_transactions
      │   └─ Set data_quality_score
      │
      ├─ [PUBLISH] publish_users_with_spark()
      │   ├─ apply_masking_users() 
      │   │   ├─ SHA256 email
      │   │   ├─ SHA256 ip_address
      │   │   └─ Máscara valores
      │   ├─ Dedup + append a prod.raw_users
      │   └─ Set data_quality_score
      │
      └─ [DBT] dbt_run()
          ├─ Ejecuta staging models
          ├─ Ejecuta intermediate models
          ├─ Ejecuta marts (fct, dim)
          └─ Genera dbt_docs/

⚡ Tiempo total: ~30-45 minutos (con paginación)
```

---

## 📡 Configuración de Conexiones

### Variables de Entorno (.env)

```bash
# API
API_KEY=your-api-key
API_BASE_URL=http://host.docker.internal:5000/api

# Database
DB_USER=user_dbt
DB_PASS=password_dbt
DB_HOST=postgres_warehouse
DB_NAME=db_warehouse
DB_PORT=5432

# Airflow
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_UID=50000

# Seguridad
PII_SALT=your-encryption-salt
```

### Conexión PostgreSQL (Airflow UI)

```
Connection ID: postgres_warehouse_conn
Host: postgres_warehouse
Port: 5432
Database: db_warehouse
User: user_dbt
Password: password_dbt
```

---

## 🔗 Dependencias y Requisitos

### Python (requirements.txt)
- `apache-airflow 2.8.1` - Orquestación
- `apache-spark 3.3.0` - Procesamiento distribuido
- `dbt-postgres 1.5.0` - Transformaciones
- `great-expectations 0.17.15` - Validación
- `pandas 1.5.2` - DataFrames
- `sqlalchemy 1.4.39` - ORM
- `psycopg2 2.9.3` - Driver PostgreSQL
- `requests 2.28.1` - Cliente HTTP
- `tenacity 8.0.1` - Retry automático
- `cryptography 41.0.0+` - Hashing/Encriptación

### Sistema (Dockerfile)
- `openjdk-17-jre-headless` - Java para Spark
- `postgresql-42.5.0.jar` - Driver JDBC

---

## 📈 Monitoreo y Observabilidad

### Logs
```
logs/
├── dag_id=dag_maestro_etl/
│   └── run_id=manual__2026-04-04T.../ 
│       └── task_id=write_to_audit/
└── dag_id=dag_wap_unl_final/
    └── run_id=manual__2026-04-04.../
```

### Reportes GX
```
gx/uncommitted/data_docs/
├── index.html (resumen)
└── suite_name/
    └── validation_result_*.html
```

### dbt Documentation
```
dbt/dbt_docs/
├── index.html (model lineage)
└── catalog.json (metadata)
```

---

## 🎓 Lecciones de Diseño Aplicadas

### ✅ Resiliencia
- Retry automático con backoff exponencial
- Manejo específico de errores (429, 500, 401)
- Truncate/Append para idempotencia

### ✅ Escalabilidad
- Spark para datos a escala
- Paginación en API (100 registros/página)
- Paralelismo en dbt (4 threads)

### ✅ Mantenibilidad
- Separación de concerns (Write/Audit/Publish)
- Código modular en scripts/
- Configuración externalizada (.env, JSON)

### ✅ Observabilidad
- Logging estructurado
- Métricas de calidad
- Data docs automáticos

### ✅ Seguridad
- PII enmascarado en prod
- Variables sensibles en .env
- Funciones de seudonimización

---

## 🔄 Ciclo de Mejora Continua

1. **Monitor** → Logs, GX validations, dbt tests
2. **Alert** → Notificaciones en Airflow UI
3. **Fix** → Ajuste de expectativas, reglas de masking
4. **Deploy** → Rebuild Docker, reiniciar servicios
5. **Validate** → Re-run DAG, verificar calidad

---

## 📞 Contacto y Soporte

**Taller**: Diseño de Procesos ETL en Data Science - UNL
**Período**: 2 - 2026
**Módulo**: Unidad 2 - Arquitectura Enterprise ETL

---

**Última Actualización**: Abril 2026  
**Versión del Documento**: 1.0
