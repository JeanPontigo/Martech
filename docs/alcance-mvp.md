# Alcance del MVP — Proyecto MarTech (ATLAS)

## 1. Objetivo del MVP

Demostrar que la migración de los ETL legacy (Python → MySQL local) a una plataforma GCP es viable, entregando un **pipeline diario funcional extremo a extremo** para los 3 tenants (PF, Carozzi, Ariztía) con datos de e-commerce modelados hasta la capa Gold y dashboards consumibles.

## 2. Alcance incluido

- **Extracción**: 3 extractores contenerizados (Cloud Run Jobs), uno por tenant, contra Magento REST API con paginación, reintentos y watermarks delta (`pipeline_state`).
- **Bronze**: ingesta append-only a `bronze.ecommerce` (+ `bronze.email_mailup` para PF) y tablas operacionales `pipeline_state` / `pipeline_logs`.
- **Orquestación mínima viable**: Cloud Scheduler (04:00 America/Santiago) → Pub/Sub (un tópico por tenant) → Cloud Function que dispara dbt Cloud.
- **Transformación dbt** (`martech_atlas`):
  - Staging: 16 vistas de normalización por tenant.
  - Silver: dimensiones (`dim_client`, `dim_company`, `dim_product`, `dim_category`, `dim_product_category`) y hechos (`fact_orders`, `fact_order_items`, `fact_email`, `fact_web_events`) incrementales con `tenant_id`.
  - Gold: 17 tablas de negocio (KPIs diarios, `customer_360` con RFM, performance de productos/categorías/campañas, sesiones web).
- **Fuentes integradas**: Magento (3 tenants), Mailup (PF), GA4 export nativo (PF).
- **Consumo**: dashboards en Looker Studio sobre la capa Gold.
- **Documentación y operación**: README completo, manual técnico, plan de pruebas y este alcance.

## 3. Fuera del alcance del MVP (fases futuras)

- Enmascaramiento dinámico de PII y vistas autorizadas por rol (exigido por Ley 21.719 antes de dic-2026).
- Migración total de secretos a Secret Manager (hoy parcialmente en variables de entorno).
- Extracción de Fidelizador (pendiente de integración).
- Modelos BigQuery ML (`churn_probability`, CLTV en `customer_360` — campos ya reservados).
- Alertas automáticas de anomalías en la ingesta.
- Tests automatizados en CI (`dbt test` en cada push).

## 4. Criterios de éxito del MVP

1. Los 3 extractores corren diariamente sin intervención manual.
2. `dbt run` completo (staging → silver → gold) termina en verde.
3. Los KPIs de `kpi_orders_daily` cuadran con Magento en muestra de 3 días.
4. Dashboards disponibles antes de las 07:00 con datos del día anterior.
5. Cero secretos versionados en el repositorio.
