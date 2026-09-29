# Sprint Backlog — Proyecto MarTech (Plataforma ATLAS)

**Equipo:** Jean Carlos Pontigo (JP) · Rodrigo Urbina (RU) · Jaime Vergara (JV)
**Duración de sprint:** 2 semanas. Los responsables rotan entre los tres integrantes.

---

## Sprint 1 — Fundaciones e ingesta PF ✅ Completado

**Objetivo:** repositorio operativo y primera extracción diaria del tenant PF.

| # | Tarea | Responsable | Estado |
|---|-------|-------------|--------|
| S1-01 | Estructura del repositorio y `.gitignore` (excluir `.env`, `__pycache__`) | JP | Completado |
| S1-02 | Extractor Magento PF: órdenes, clientes, productos → `bronze.ecommerce` | RU | Completado |
| S1-03 | Tablas `pipeline_state` y `pipeline_logs` para watermarks y bitácora | JV | Completado |
| S1-04 | `Dockerfile` y `requirements.txt` del pipeline PF | JP | Completado |
| S1-05 | Prueba de carga inicial y validación de conteos contra Magento | RU | Completado |

## Sprint 2 — Transformación dbt y tenant Carozzi ✅ Completado

**Objetivo:** capa Silver/Gold funcional y segundo tenant con email.

| # | Tarea | Responsable | Estado |
|---|-------|-------------|--------|
| S2-01 | `dbt_project.yml`: proyecto `martech_atlas`, materializaciones por capa | JV | Completado |
| S2-02 | Modelos staging `stg_pf__*` y `stg_mc__*` (parseo de JSON crudo) | RU | Completado |
| S2-03 | Silver: `dim_client`, `dim_product`, `fact_orders`, `fact_order_items`, `fact_email` | JP | Completado |
| S2-04 | Extractor Mailup → `bronze.email_mailup` y topic `pipeline-carozzi` | RU | Completado |
| S2-05 | Incidente: Cloudflare bloqueaba las llamadas a Cloud Run — diagnóstico y mitigación | JV | Completado |
| S2-06 | Macros `normalize_string`, `ga4_param`, `generate_schema_name` | JP | Completado |

## Sprint 3 — Tenant Ariztía, Gold y visualización 🔄 En curso

**Objetivo:** tercer tenant operativo y modelos de negocio consumibles.

| # | Tarea | Responsable | Estado |
|---|-------|-------------|--------|
| S3-01 | Extractor Magento Ariztía (`pipelines/aatn`) con credenciales por entorno | RU | Completado |
| S3-02 | Staging `stg_aatn__*` y extensión de Silver a 3 tenants | JP | En curso |
| S3-03 | Modelos Gold: `kpi_orders_daily`, `customer_360`, `campaign_performance`, `email_performance` | JV | En curso |
| S3-04 | Dashboards Looker Studio por tenant sobre capa Gold | RU | Pendiente |
| S3-05 | Corrección: `.env` quedaba dentro de la imagen Docker — agregar `.dockerignore` y reconstruir | JP | En curso |
| S3-06 | Estabilizar trigger post-extracción (la Cloud Function resultó poco confiable como disparador) | JV | En curso |

## Sprint 4 — Endurecimiento y cierre 📋 Planificado

**Objetivo:** plataforma auditable y lista para entrega.

| # | Tarea | Responsable | Estado |
|---|-------|-------------|--------|
| S4-01 | Migrar credenciales restantes a Secret Manager y rotar tokens expuestos | JP | Pendiente |
| S4-02 | Tests dbt (`unique`, `not_null`) y `schema.yml` en modelos críticos | RU | Pendiente |
| S4-03 | Pipeline transitorio de Carozzi desde VPS: plan de migración definitiva a Cloud Run | JV | Pendiente |
| S4-04 | Documento de arquitectura y manual de operación | JP | Pendiente |
| S4-05 | Revisión de Definition of Done contra cada historia del backlog | RU | Pendiente |

---

**Notas del equipo:**
- Las historias HU-01 a HU-11 del Product Backlog están cubiertas entre los sprints 1 y 3; HU-12 y HU-13 quedan para el sprint 4.
- Los incidentes (S2-05, S3-05, S3-06) se incorporaron al sprint en curso en cuanto aparecieron, siguiendo el principio ágil de responder al cambio.
