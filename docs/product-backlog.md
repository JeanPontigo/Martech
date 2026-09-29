# Product Backlog — Proyecto MarTech (Plataforma ATLAS)

**Equipo:** Jean Carlos Pontigo · Rodrigo Urbina · Jaime Vergara
**Ordenado por prioridad.** P1 = crítico para el MVP, P2 = importante, P3 = mejora.

---

## Épica 1: Ingesta y capa Bronze

**HU-01 (P1) — Extractor Magento tenant PF**
- *Como* ingeniero de datos, *quiero* extraer órdenes, clientes y productos de Magento del tenant PF a BigQuery, *para* reemplazar la carga manual actual.
- **Criterios de aceptación:** los datos crudos quedan en `bronze.ecommerce` con `tenant_id = 'pf'`; la ejecución registra estado y conteos en `pipeline_state` / `pipeline_logs`; el job corre como Cloud Run Job sin intervención manual.

**HU-02 (P1) — Control de ejecuciones (watermarks)**
- *Como* operador de la plataforma, *quiero* que cada extracción guarde su punto de avance (watermark), *para* no reprocesar todo el historial en cada corrida diaria.
- **Criterios de aceptación:** el watermark se actualiza solo si la extracción de la entidad termina sin errores; una re-ejecución retoma desde el último punto válido.

**HU-03 (P1) — Extractor Magento tenant Ariztía**
- *Como* ingeniero de datos, *quiero* replicar la extracción para el tenant Ariztía, *para* cubrir el tercer tenant del proyecto.
- **Criterios de aceptación:** mismos criterios que HU-01 con `tenant_id = 'aatn'`; credenciales por variables de entorno, nunca en el código.

**HU-04 (P2) — Extractor email tenant Carozzi (Mailup)**
- *Como* analista de marketing, *quiero* los datos de campañas de email de Carozzi en Bronze, *para* medir performance de campañas.
- **Criterios de aceptación:** datos crudos en `bronze.email_mailup`; topics Pub/Sub `pipeline-carozzi` notifica el término de la extracción.

**HU-05 (P2) — Orquestación por Pub/Sub**
- *Como* ingeniero de datos, *quiero* que al terminar cada extracción se publique un mensaje en Pub/Sub, *para* disparar la transformación dbt automáticamente.
- **Criterios de aceptación:** el mensaje incluye tenant, entidad, estado y conteos; no se dispara la transformación si la extracción falló por completo.

## Épica 2: Transformación (dbt — Silver y Gold)

**HU-06 (P1) — Modelos staging por tenant**
- *Como* modelador de datos, *quiero* vistas staging que parseen el JSON crudo de Bronze, *para* tener una base tipada y limpia por tenant.
- **Criterios de aceptación:** `stg_pf__*`, `stg_mc__*` y `stg_aatn__*` compilan en dbt (`dbt build` sin errores) y exponen clientes, órdenes, ítems y productos.

**HU-07 (P1) — Dimensiones y hechos Silver**
- *Como* modelador de datos, *quiero* dimensiones (`dim_client`, `dim_product`, `dim_category`) y hechos (`fact_orders`, `fact_order_items`, `fact_email`, `fact_web_events`) incrementales, *para* tener un modelo analítico consistente.
- **Criterios de aceptación:** materialización incremental con `merge` y `unique_key` compuesta incluyendo `tenant_id`; re-ejecuciones no duplican filas.

**HU-08 (P1) — Modelos Gold de negocio**
- *Como* jefe comercial, *quiero* tablas Gold con KPIs diarios, customer 360 y performance de campañas, *para* tomar decisiones con datos confiables.
- **Criterios de aceptación:** `kpi_orders_daily`, `customer_360`, `campaign_performance`, `email_performance`, `product_performance` y `web_sessions_daily` se materializan como tablas y cuadran con los totales de Silver.

**HU-09 (P2) — Fuentes GA4 en Silver**
- *Como* analista digital, *quiero* los eventos web de GA4 modelados en `fact_web_events`, *para* cruzar comportamiento web con ventas.
- **Criterios de aceptación:** la fuente `ga4_pf.events_*` queda declarada en `sources.yml` y el modelo incremental procesa solo particiones nuevas.

## Épica 3: Consumo y operación

**HU-10 (P2) — Dashboards Looker Studio por tenant**
- *Como* usuario de negocio, *quiero* dashboards conectados a la capa Gold, *para* ver ventas, clientes y campañas sin pedir reportes manuales.
- **Criterios de aceptación:** al menos un dashboard por tenant con datos del día anterior disponibles antes de las 08:00.

**HU-11 (P2) — Dockerfile y despliegue por pipeline**
- *Como* ingeniero de datos, *quiero* cada pipeline empaquetado en su Dockerfile con `.dockerignore`, *para* desplegar versiones reproducibles en Cloud Run.
- **Criterios de aceptación:** la imagen no incluye `.env`, `__pycache__` ni credenciales; el contenedor ejecuta el `main` del tenant correspondiente.

**HU-12 (P3) — Tests y documentación dbt**
- *Como* responsable de calidad, *quiero* tests dbt (`unique`, `not_null`, relaciones) y `schema.yml` documentado, *para* detectar datos rotos antes de que lleguen a Gold.
- **Criterios de aceptación:** `dbt test` corre en el pipeline y falla la corrida si un test crítico no pasa.

**HU-13 (P3) — Enmascaramiento de datos personales**
- *Como* encargado de cumplimiento, *quiero* RUT, email y nombre enmascarados antes de la capa Gold, *para* cumplir la Ley 21.719.
- **Criterios de aceptación:** ningún modelo Gold expone PII en texto plano; el acceso a datos sin enmascarar queda restringido y auditado.
