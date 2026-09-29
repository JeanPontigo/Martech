# Plan de pruebas — Proyecto MarTech (ATLAS)

## 1. Estrategia

Pruebas por capa del pipeline, de izquierda a derecha: extracción → Bronze → dbt (staging/silver/gold) → consumo. Sin `schema.yml` con tests en el repo aún: este plan define el piso mínimo exigible.

## 2. Pruebas de extracción (pipelines/)

| ID | Caso | Resultado esperado |
|---|---|---|
| EXT-001 | Corrida `--entity all` en ambiente de prueba | Todas las entidades terminan en `success` en `pipeline_logs` |
| EXT-002 | Corte de red a mitad de la paginación | Reintento con backoff; el watermark no avanza en la entidad fallida |
| EXT-003 | Segunda corrida sin datos nuevos | 0 registros nuevos; Bronze no crece; estado `success` |
| EXT-004 | `--entity orders` aislado | Solo se extrae `orders`; el resto de entidades intactas |
| EXT-005 | Token inválido | Error controlado y logueado; sin stacktrace con secretos en logs |

## 3. Pruebas de datos Bronze

| ID | Caso | Resultado esperado |
|---|---|---|
| BRZ-001 | Esquema de `bronze.ecommerce` | Columnas `id`, `tenant_id`, `entity`, `raw_json`, `ingested_at` presentes |
| BRZ-002 | `raw_json` válido | 100% de filas parseables como JSON |
| BRZ-003 | No duplicados por re-ejecución | Clave `(tenant_id, entity, id)` única tras reproceso (o deduplicación documentada) |

## 4. Pruebas dbt (staging / silver / gold)

| ID | Caso | Resultado esperado |
|---|---|---|
| DBT-001 | `dbt compile` | 0 errores de sintaxis en los 43 modelos |
| DBT-002 | `dbt run --select staging` | Las 16 vistas se crean sin error |
| DBT-003 | `dbt run --select silver` (2 corridas) | Segunda corrida incremental: 0 merges (sin cambios) |
| DBT-004 | Unicidad de PKs | `unique` test en `tenant_id + client_id` (dim_client), `tenant_id + order_id` (fact_orders), etc. |
| DBT-005 | Integridad referencial | `relationships` test: `fact_orders.client_id → dim_client`, `fact_order_items.order_id → fact_orders` |
| DBT-006 | Aislamiento multi-tenant | Ningún `tenant_id` cruza a otro en joins (revisar `customer_360` y gold) |
| DBT-007 | `dbt test` completo | 100% de tests genéricos en verde antes de cada release |

## 5. Pruebas de aceptación (negocio)

| ID | Caso | Resultado esperado |
|---|---|---|
| ACE-001 | KPIs diarios en Looker Studio | `kpi_orders_daily` cuadra con el total de Magento del día (muestra de 3 días) |
| ACE-002 | RFM `customer_360` | Quintiles 1–5 en las 3 dimensiones, sin nulos en clientes con compras |
| ACE-003 | Frescura del dato | Dashboard del día disponible antes de las 07:00 |

## 6. Pruebas no funcionales

- **Seguridad**: escanear el repo e imágenes Docker en busca de secretos (`gitleaks`); verificar `.dockerignore` excluye `.env`.
- **Cumplimiento**: validar enmascaramiento de PII (email, RUT) en Gold y vistas de BI (Ley 21.719).
- **Recuperación**: borrar silver/gold de prueba y reconstruir solo desde Bronze.
