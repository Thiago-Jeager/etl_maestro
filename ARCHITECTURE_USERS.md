# 🏗️ Arquitectura WAP Completa - Transacciones + Usuarios

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          🌐 EXTERNAL APIs                                   │
├──────────────────────────────┬──────────────────────────────────────────────┤
│    POST /api/transactions    │    POST /api/users (NUEVO)                   │
│  (16 campos monetarios)      │  (5 campos + PII: email, ip)                 │
└──────────────────┬───────────┴────────────────────────┬─────────────────────┘
                   │                                    │
                   ▼                                    ▼
      ┌────────────────────────┐        ┌────────────────────────┐
      │ WRITE → API Extraction │        │ WRITE → API Extraction │
      │                        │        │                        │
      │ write_to_audit()       │        │ write_users_to_audit() │
      │  • Retry logic (429)   │        │  • Retry logic         │
      │  • Paginación          │        │  • 100 records/page    │
      └────────────┬───────────┘        └────────────┬───────────┘
                   │                                  │
                   ▼                                  ▼
     ┌──────────────────────────────┐  ┌──────────────────────────────┐
     │ audit.raw_transactions       │  │ audit.raw_users ✅ PII       │
     │ ─────────────────────────────│  │ ──────────────────────────────│
     │ • transaction_id (PK)        │  │ • user_id (PK)               │
     │ • user_id (FK)               │  │ • first_name, last_name      │
     │ • amount (16,2)              │  │ • email (5% NULL intencional)│
     │ • status (enum)              │  │ • ip_address (auditoría)     │
     │ • gx_validation_status       │  │ • country                    │
     │ • ingested_at (timestamp)    │  │ • registration_date          │
     └────────────┬──────────────────┘  └────────────┬─────────────────┘
                  │                                   │
                  ▼                                   ▼
     ┌────────────────────────────┐  ┌────────────────────────────────┐
     │ AUDIT → Great Expectations │  │ AUDIT → Great Expectations ✅ │
     │                            │  │                                │
     │ audit_with_gx()            │  │ audit_users_with_gx()          │
     │ ✓ transactions_critical    │  │ ✓ users_critical (bloqueante)  │
     │   (172 registros fallaron) │  │ ✓ users_warnings (5% emails)   │
     │ ✓ transactions_warnings    │  │ • email: 5% nulos OK          │
     │   (no bloquea)             │  │ • ip_address: regex validación │
     │                            │  │ • country/names: no nulos      │
     └────────────┬───────────────┘  └────────────┬───────────────────┘
                  │                               │
                  ▼                               ▼
     ┌────────────────────────────┐  ┌──────────────────────────────┐
     │ PUBLISH → Spark + Masking  │  │ PUBLISH → Spark + SHA256 ✅  │
     │                            │  │                              │
     │ publish_with_spark()       │  │ publish_users_with_spark()   │
     │ • Filtros: COMPLETED      │  │ • Enmascaramiento:           │
     │   amount > 0              │  │   ├─ email_hashed (SHA256)   │
     │ • Spark DataFrame ops     │  │   └─ ip_address_hashed (SHA2)│
     │ • apply_masking()         │  │ • Deduplicación por user_id  │
     │ • Dedup + append          │  │ • data_quality_score = 1.0   │
     └────────────┬───────────────┘  └────────────┬─────────────────┘
                  │                               │
                  ▼                               ▼
     ┌────────────────────────────────┐ ┌──────────────────────────────┐
     │ prod.raw_transactions ✅       │ │ prod.raw_users 🔐            │
     │ ─────────────────────────────  │ │ ────────────────────────────  │
     │ • Datos LIMPIOS + validados    │ │ • first_name, last_name      │
     │ • data_quality_score: 1.0      │ │ • email_hashed (SHA256)      │
     │ • processed_at: timestamp      │ │ • ip_address_hashed (SHA256) │
     │ • Sin PII (transaccional)      │ │ • PII enmascarado ✅         │
     └────────────┬───────────────────┘ └────────────┬──────────────────┘
                  │                                   │
                  └──────────┬────────────────────────┘
                             │
                             ▼
            ┌─────────────────────────────────┐
            │ DBT TRANSFORMATION PIPELINE 📊  │
            ├─────────────────────────────────┤
            │ STAGING (Normalización)         │
            │ ├─ stg_raw_transactions         │
            │ └─ stg_raw_users (PII hashed)  │
            │                                 │
            │ INTERMEDIATE (Enriquecimiento) │
            │ ├─ int_transactions_enriched   │
            │ └─ int_users_enriched (RFM)   │
            │                                 │
            │ MARTS/ANALYTICS (Business)    │
            │ ├─ fct_transactions            │
            │ └─ dim_users (VIP/Premium/etc) │
            └─────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                  📋 ASEGURAMIENTO DE CALIDAD (GX)                            │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  TRANSACCIONES:                    │  USUARIOS:                             │
│  ✓ Críticas (bloquea):             │  ✓ Críticas (bloquea):                 │
│    • PK único (transaction_id)     │    • PK único (user_id)                │
│    • FK no nulo (user_id)          │    • Names no nulos                    │
│    • amount positivo               │    • IP no nulo (auditoría)            │
│    • status en {COMPLETED,...}     │    • registration_date no nulo        │
│    • currency válida               │    • IP formato IPv4/IPv6              │
│                                    │                                        │
│  ⚠️ Advertencias (continúa):       │  ⚠️ Advertencias (continúa):           │
│    • NULLs en valores              │    • Email: 5% nulos ESPERADO          │
│    • Outliers de monto             │    • Format email válido               │
│                                    │    • country: 2-50 chars               │
│                                    │                                        │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                    🔐 CUMPLIMIENTO LDPD / GDPR                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  CAMPO         │ SENSIBILIDAD │ MÉTODO           │ IMPLEMENTACIÓN           │
│  ─────────────┼──────────────┼──────────────────┼───────────────────────   │
│  email        │ CRÍTICA      │ SHA256 + salt    │ prod.raw_users.email_*  │
│  ip_address   │ ALTA         │ SHA256 + salt    │ prod.raw_users.ip_*     │
│  user_id      │ DIRECTA      │ Masked views     │ audit.v_masked_users    │
│  first_name   │ MEDIA        │ Datos limpios    │ stg_raw_users           │
│  country      │ BAJA         │ Sin enmascarar   │ Disponible              │
│                                                                               │
│  Retención: email 180 días | IP 90 días | Transacciones 365 días            │
│  Vistas:    dev (ofuscadas) | analyst (hashed) | admin (full acceso)        │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                      🎯 OBJETIVOS PEDAGÓGICOS                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  1. COMPLETITUD (5% nulos en email):                                        │
│     ✅ Detectado en users_warnings_suite.json (mostly: 0.95)                │
│     ✅ Métrica en dim_users.completeness_flag                              │
│                                                                               │
│  2. ENMASCARAMIENTO DE PII:                                                 │
│     ✅ SHA256 en fase PUBLISH (Spark) para email e IP                      │
│     ✅ Separación: audit (crudos) vs prod (enmascarados)                   │
│     ✅ Auditable pero irreversible                                         │
│                                                                               │
│  3. VALIDACIÓN CON GX:                                                      │
│     ✅ Dos suites (críticas + advertencias)                                │
│     ✅ Bloqueos automáticos en fallos críticos                             │
│     ✅ Logs en audit.gx_validation_logs                                    │
│                                                                               │
│  4. TRANSFORMACIÓN CON DBT:                                                 │
│     ✅ Staging: Normalización y limpieza                                   │
│     ✅ Intermediate: Join con transacciones, métricas RFM                  │
│     ✅ Marts: Segmentación de clientes, churn risk analysis               │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Próximas Ejecuciones

```sql
-- 1. Verificar extracción de usuarios
SELECT COUNT(*) as total_users,
       COUNT(CASE WHEN email IS NULL THEN 1 END) as null_emails,
       ROUND(100.0 * COUNT(CASE WHEN email IS NULL THEN 1 END) / COUNT(*), 2) as null_pct
FROM audit.raw_users;

-- 2. Validar enmascaramiento en PROD
SELECT user_id, 
       email_hashed LIKE 'a%' as is_hashed_email,
       LENGTH(email_hashed) as hash_length
FROM prod.raw_users LIMIT 10;

-- 3. Ver segmentación RFM
SELECT customer_segment, churn_risk_level, COUNT(*) as count
FROM analytics.dim_users
GROUP BY customer_segment, churn_risk_level
ORDER BY count DESC;

-- 4. Revisar validaciones GX
SELECT table_name, success_rate, critical_failures
FROM audit.gx_validation_logs
WHERE table_name = 'audit.raw_users'
ORDER BY validation_timestamp DESC LIMIT 5;
```
