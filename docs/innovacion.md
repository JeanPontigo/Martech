# Innovación — Proyecto MarTech (ATLAS)

## 1. Punto de partida: el sistema legacy

Antes de ATLAS, Martech operaba con scripts Python artesanales por tenant que extraían de Magento y cargaban a un **MySQL local**:

- Sin escalabilidad: el disco local y la memoria eran el techo.
- Sin auditoría: sin bitácora de ejecuciones ni watermarks; ante un fallo no se sabía qué se había cargado.
- Acoplado: extracción y carga en el mismo script; un error detenía todo.
- Datos sin modelar: tablas planas por tenant, sin dimensiones ni hechos, sin capa de negocio.
- Riesgo de seguridad: credenciales en archivos locales y `.env` dentro de imágenes.

## 2. Qué innova ATLAS

| Aspecto | Legacy | ATLAS |
|---|---|---|
| Plataforma | MySQL local | BigQuery serverless (escala sin operar servidores) |
| Arquitectura | Script monolítico por tenant | Arquitectura medallion Bronze → Silver → Gold |
| Orquestación | Ejecución manual | Pipeline diario automático (Scheduler → Cloud Run → Pub/Sub → dbt) |
| Idempotencia y reintentos | No | Watermarks por entidad, retry con backoff, bitácora en `pipeline_state`/`pipeline_logs` |
| Multi-tenant | Bases separadas ad-hoc | Aislamiento lógico por `tenant_id` en llaves compuestas |
| Modelado | Tablas planas | 9 modelos silver (dims/hechos) + 17 modelos gold con RFM, KPIs y cohortes |
| Identidad B2B | No existía | `fact_web_events` une GA4 con `company_id` (decisión de identidad a nivel empresa) |
| Email marketing | Fuera del warehouse | `fact_email` integra Mailup con métricas de apertura/clic por campaña |
| Observabilidad | Nula | Logs estructurados por tenant/entidad/estado |
| Despliegue | Copia de scripts | Imágenes Docker por tenant + docker-compose para desarrollo local |

## 3. Diferenciales técnicos

1. **Overlay de status (Ariztía)**: `fact_orders` re-trae la fila base completa cuando llega un cambio de estado fuera de la ventana incremental, evitando que el `MERGE` deje campos en NULL — solución a un problema real de datos.
2. **Columnas protegidas en `fact_email`**: el nombre de campaña se carga manualmente y el merge nunca lo sobrescribe (`merge_update_columns`).
3. **Normalización lingüística**: macro `normalize_string` para búsquedas insensibles a tildes en español.
4. **Seed geográfico chileno**: `comunas_regiones.csv` estandariza la dimensión geográfica a la división administrativa real.

## 4. Líneas futuras de innovación

- Enmascaramiento dinámico de PII y vistas autorizadas (Ley 21.719).
- Detección de anomalías en la ingesta diaria (alertas automáticas).
- BigQuery ML para `churn_probability` y CLTV en `customer_360` (campos ya reservados).
