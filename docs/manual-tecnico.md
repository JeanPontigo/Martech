# Manual técnico — Proyecto MarTech (ATLAS)

## 1. Despliegue

### 1.1 Extractores (Cloud Run Jobs)

```bash
# Construir y subir cada imagen (ejemplo PF)
docker build -t gcr.io/martech-data-platform-atlas/pipeline-pf:latest ./pipelines/pf
docker push gcr.io/martech-data-platform-atlas/pipeline-pf:latest

# Crear el Job en Cloud Run (se repite por tenant)
gcloud run jobs create pipeline-pf \
  --image gcr.io/martech-data-platform-atlas/pipeline-pf:latest \
  --region southamerica-west1 \
  --set-env-vars TENANT_ID=pf,GCP_PROJECT_ID=martech-data-platform-atlas \
  --set-secrets PF_API_TOKEN=pf-api-token:latest
```

### 1.2 Scheduler

Crear un trabajo diario por tenant en Cloud Scheduler: `0 4 * * *` (zona `America/Santiago`) que invoque cada Cloud Run Job.

### 1.3 dbt Cloud

- Proyecto `martech_atlas`, perfil `martech_atlas`.
- Job diario gatillado por la Cloud Function al recibir el mensaje de Pub/Sub.
- Secuencia: `dbt seed` → `dbt run` → `dbt test`.

### 1.4 Infraestructura BigQuery

Datasets: `bronze`, `silver`, `gold` (+ `analytics_479606051` para el export nativo de GA4). Las tablas operacionales `pipeline_state` y `pipeline_logs` se crean en el dataset Bronze en la primera ejecución.

## 2. Configuración

Todas las variables están documentadas en `.env.example` (sin valores). Para desarrollo local:

```bash
cp .env.example .env   # completar y NO commitear
docker-compose build
docker-compose run --rm pipeline-pf
```

Comandos útiles por pipeline:

```bash
# Extracción completa (todas las entidades)
python3 main_gcp.py --entity all

# Solo una entidad
python3 main_gcp.py --entity orders

# Carga histórica completa (cuando el extractor lo soporta)
python3 main_gcp.py --entity all --full-load
```

## 3. Operación diaria

1. **04:00** — Scheduler dispara los 3 jobs.
2. Cada extractor: lee watermark → extrae delta de Magento → `WRITE_APPEND` a `bronze.ecommerce` → actualiza `pipeline_state`/`pipeline_logs` → publica a Pub/Sub (`pipeline-pf`, `pipeline-carozzi`, `pipeline-ariztia`).
3. Cloud Function recibe los mensajes y dispara dbt Cloud.
4. dbt reconstruye staging (vistas), hace merge incremental en silver y refresca las 17 tablas gold.
5. Looker Studio refleja datos del día (~06:00).

### Monitoreo

```sql
-- Estado de la última ejecución por tenant/entidad
SELECT tenant_id, entity, status, records, started_at, finished_at
FROM `martech-data-platform-atlas.bronze.pipeline_logs`
ORDER BY started_at DESC
LIMIT 50;

-- Watermarks actuales
SELECT * FROM `martech-data-platform-atlas.bronze.pipeline_state`;
```

## 4. Troubleshooting

| Síntoma | Causa probable | Acción |
|---|---|---|
| Job termina en `error` para una entidad | API del tenant caída o token expirado | Revisar `pipeline_logs`; rotar token en Secret Manager; la próxima corrida reintenta el delta |
| `dbt run` falla en silver | Error de compilación o `ref` inexistente | Correr `dbt compile` local y revisar el modelo indicado |
| Duplicados en Bronze | Re-ejecución con bordes inclusivos | Deduplicar por `(tenant_id, entity, id)` antes del staging |
| Dashboard sin datos del día | dbt no corrió o Pub/Sub no disparó | Verificar mensaje en el tópico y logs de la Cloud Function |
| `429 Too Many Requests` de Magento | Throttling | El extractor reintenta con backoff automáticamente; si persiste, ampliar la ventana |
| Credenciales expuestas | Token commiteado por error | **Rotar inmediatamente** el token y purgar del historial de git |
