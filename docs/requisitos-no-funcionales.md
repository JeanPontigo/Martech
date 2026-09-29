# Requisitos no funcionales — Proyecto MarTech (ATLAS)

| ID | Categoría | Requisito | Criterio de aceptación |
|---|---|---|---|
| RNF-001 | Rendimiento | La extracción diaria completa (3 tenants) debe terminar en < 2 horas. | `pipeline_logs` muestra duración total < 120 min. |
| RNF-002 | Rendimiento | Los modelos silver incrementales procesan solo el delta diario. | `dbt run` de silver < 30 min en día típico. |
| RNF-003 | Rendimiento | Dashboards de Looker Studio responden en < 10 s. | Gold pre-agregada; sin consultas ad-hoc a Bronze desde BI. |
| RNF-004 | Disponibilidad | Pipeline disponible para su ventana diaria 04:00–06:00. | Cloud Scheduler con reintento automático ante fallo del job. |
| RNF-005 | Disponibilidad | Ante fallo de una entidad, el resto del pipeline continúa. | `pipeline_logs` registra estado por entidad; el watermark solo avanza en entidades OK. |
| RNF-006 | Escalabilidad | Agregar un nuevo tenant no exige rediseño: nuevo extractor + `tenant_id`. | El modelo multi-tenant aísla por `tenant_id` en todas las llaves. |
| RNF-007 | Escalabilidad | BigQuery escala el almacenamiento sin intervención (Bronze append-only). | Sin límites locales de disco como en el MySQL anterior. |
| RNF-008 | Seguridad | Ningún secreto versionado en el repo ni en imágenes Docker. | Credenciales por variables de entorno / Secret Manager; `.env` en `.gitignore`; `.dockerignore` en imágenes. |
| RNF-009 | Seguridad | Acceso a BigQuery con principio de mínimo privilegio (cuentas de servicio por componente). | Roles IAM acotados por dataset. |
| RNF-010 | Trazabilidad | Toda ejecución queda registrada (estado, conteos, errores). | Tablas `pipeline_state` y `pipeline_logs` en Bronze. |
| RNF-011 | Recuperabilidad | Ante fallo, es posible reconstruir Silver/Gold desde Bronze. | Bronze conserva el JSON crudo original intacto. |
| RNF-012 | Cumplimiento | Datos personales (email, nombre, RUT) protegidos según **Ley 21.719** (vigente 2026-12-01). | Enmascaramiento a nivel de columnas en BigQuery antes de la capa Gold; vistas autorizadas por rol; sin PII en logs. |

## Notas de cumplimiento (Ley 21.719)

- La capa Bronze conserva datos crudos con PII por necesidad operativa (re-procesamiento); el acceso se restringe por IAM.
- La capa Gold y los dashboards deben exponer PII solo a roles autorizados, con enmascaramiento dinámico (`MASK` / vistas autorizadas).
- Los tokens de API de los tenants se rotan periódicamente y viven en Secret Manager, nunca en código.
