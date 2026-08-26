"""
utils_gcp.py — Pipeline GCP puro
==================================
Responsabilidad: funciones compartidas para el flujo ELT en GCP.

Diferencias respecto a utils.py (legacy MySQL):
- Sin SQLAlchemy ni PyMySQL — 100% BigQuery client
- pipeline_state → bronze.pipeline_state (MERGE para upsert)
- pipeline_logs  → bronze.pipeline_logs  (WRITE_APPEND)
- build_url y build_headers → sin cambios
- normalizar_rut → sin cambios

Tablas BigQuery requeridas (se crean automáticamente en primera escritura):
  bronze.pipeline_state  → tenant_id, entity, last_sync, updated_at
  bronze.pipeline_logs   → id, tenant_id, entity, status, records_extracted,
                           duration_sec, date_from, date_to, error_msg, executed_at
"""

import logging
import os
import uuid
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

log = logging.getLogger("pipeline.utils_gcp")

DEFAULT_FALLBACK = "2020-01-01 00:00:00"

# ---------------------------------------------------------------------------
# BigQuery client — singleton por proceso
# ---------------------------------------------------------------------------
def _get_bq_client() -> bigquery.Client:
    """
    Retorna un cliente BigQuery autenticado.
    Usa GOOGLE_APPLICATION_CREDENTIALS del entorno o Application Default Credentials.
    """
    load_dotenv(override=True)
    project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    return bigquery.Client(project=project)

def _get_dataset() -> str:
    load_dotenv(override=True)
    return os.getenv("BQ_DATASET", "bronze")

def _table_ref(table_name: str) -> str:
    load_dotenv(override=True)
    project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset = _get_dataset()
    return f"`{project}.{dataset}.{table_name}`"

# ---------------------------------------------------------------------------
# pipeline_state — delta loading por tenant + entidad
# ---------------------------------------------------------------------------
def get_last_sync(tenant_id: str, entity: str) -> str:
    """
    Lee el último timestamp sincronizado para el tenant + entidad.
    Retorna DEFAULT_FALLBACK si no existe registro.
    """
    client = _get_bq_client()
    query = f"""
        SELECT last_sync
        FROM {_table_ref("pipeline_state")}
        WHERE tenant_id = @tenant_id
          AND entity    = @entity
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tenant_id", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("entity",    "STRING", entity),
        ]
    )
    try:
        results = client.query(query, job_config=job_config).result()
        for row in results:
            value = str(row["last_sync"])
            log.info(f"[state] {tenant_id}.{entity} → last_sync={value}")
            return value
    except Exception as e:
        log.warning(f"[state] No se pudo leer pipeline_state: {e} — usando fallback")
    return DEFAULT_FALLBACK


def update_last_sync(tenant_id: str, entity: str, new_value: str, checkpoint_type: str = "datetime"):
    """
    MERGE (upsert) del checkpoint en pipeline_state.

    checkpoint_type:
      "datetime" → new_value es una fecha "2026-06-14 23:59:59"
                   usado por: orders, clients, products, order_items
      "id"       → new_value es un entero como string "82629"
                   usado por: companies, clients_id

    Crea la tabla si no existe con schema explícito.
    """
    client   = _get_bq_client()
    project  = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset  = _get_dataset()
    now      = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    _ensure_pipeline_state_table(client, project, dataset)

    query = f"""
        MERGE `{project}.{dataset}.pipeline_state` AS target
        USING (
            SELECT
                @tenant_id       AS tenant_id,
                @entity          AS entity,
                @new_value       AS last_sync,
                @checkpoint_type AS checkpoint_type,
                CAST(@now AS DATETIME) AS updated_at
        ) AS source
        ON target.tenant_id = source.tenant_id
       AND target.entity    = source.entity
        WHEN MATCHED THEN
            UPDATE SET
                last_sync       = source.last_sync,
                checkpoint_type = source.checkpoint_type,
                updated_at      = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (tenant_id, entity, last_sync, checkpoint_type, updated_at)
            VALUES (source.tenant_id, source.entity, source.last_sync,
                    source.checkpoint_type, source.updated_at)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tenant_id",       "STRING", tenant_id),
            bigquery.ScalarQueryParameter("entity",          "STRING", entity),
            bigquery.ScalarQueryParameter("new_value",       "STRING", new_value),
            bigquery.ScalarQueryParameter("checkpoint_type", "STRING", checkpoint_type),
            bigquery.ScalarQueryParameter("now",             "STRING", now),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        log.info(f"[state] {tenant_id}.{entity} [{checkpoint_type}] → {new_value}")
    except Exception as e:
        log.error(f"[state] Error actualizando pipeline_state: {e}")


def _ensure_pipeline_state_table(client: bigquery.Client, project: str, dataset: str):
    """Crea pipeline_state si no existe."""
    table_ref = f"{project}.{dataset}.pipeline_state"
    schema = [
        bigquery.SchemaField("tenant_id",       "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("entity",          "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("last_sync",       "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("checkpoint_type", "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("updated_at",      "DATETIME", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    try:
        client.create_table(table)
        log.info(f"[state] Tabla {table_ref} creada.")
    except Exception:
        pass  # Ya existe — ignorar


# ---------------------------------------------------------------------------
# pipeline_logs — observabilidad en BigQuery
# ---------------------------------------------------------------------------
def write_log(
    tenant_id:         str,
    entity:            str,
    status:            str,
    records_extracted: int   = 0,
    duration_sec:      float = 0.0,
    date_from:         str   = None,
    date_to:           str   = None,
    error_msg:         str   = None,
):
    """
    Registra cada ejecución en bronze.pipeline_logs.
    Permite construir en Looker Studio:
      - Última sync exitosa por tenant + entidad
      - Horas sin datos (NOW() - MAX(executed_at))
      - Volumen extraído en el tiempo
      - Tasa de errores por tenant
      - Latencia promedio
    """
    client  = _get_bq_client()
    project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset = _get_dataset()
    now     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    rows = [{
        "id":                str(uuid.uuid4()),
        "tenant_id":         tenant_id,
        "entity":            entity,
        "status":            status,
        "records_extracted": records_extracted,
        "duration_sec":      round(duration_sec, 2),
        "date_from":         date_from,
        "date_to":           date_to,
        "error_msg":         str(error_msg)[:2000] if error_msg else None,
        "executed_at":       now,
    }]

    df       = pd.DataFrame(rows)
    table_ref = f"{project}.{dataset}.pipeline_logs"

    schema = [
        bigquery.SchemaField("id",                "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("tenant_id",         "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("entity",            "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("status",            "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("records_extracted", "INT64",    mode="NULLABLE"),
        bigquery.SchemaField("duration_sec",      "FLOAT64",  mode="NULLABLE"),
        bigquery.SchemaField("date_from",         "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("date_to",           "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("error_msg",         "STRING",   mode="NULLABLE"),
        bigquery.SchemaField("executed_at",       "DATETIME", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        log.info(f"[log] {tenant_id}.{entity} → {status} | {records_extracted:,} registros | {duration_sec:.1f}s")
    except Exception as e:
        log.error(f"[log] Error escribiendo pipeline_logs: {e}")


# ---------------------------------------------------------------------------
# URL y headers de la API Magento — sin cambios respecto a utils.py
# ---------------------------------------------------------------------------
def build_url(endpoint: str) -> str:
    """Construye la URL base + endpoint."""
    load_dotenv(override=True)
    base     = os.getenv("PF_URL", "https://tiendapfalimentos.cl").rstrip("/")
    endpoint = endpoint.lstrip("/")
    return f"{base}/{endpoint}"


def build_headers() -> dict:
    """Headers de autenticación desde .env."""
    load_dotenv(override=True)
    token = os.getenv("PF_API_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


# ---------------------------------------------------------------------------
# Normalización de RUT — sin cambios
# ---------------------------------------------------------------------------
def normalizar_rut(rut) -> str | None:
    """Formato #####-x — sin puntos, con guión, k minúscula."""
    if pd.isnull(rut):
        return None
    v = str(rut).replace(".", "").lower().strip()
    if not v:
        return None
    if "-" not in v and len(v) > 1:
        v = f"{v[:-1]}-{v[-1]}"
    return v


# ---------------------------------------------------------------------------
# Banner de inicio
# ---------------------------------------------------------------------------
def ci_art():
    art = r"""
    =====================================================================================================
    ____  ____   ____  ____  ____
    |  _ \|  __| |  _ \|_  / |  _ \
    | |_) | |_   | |_) |/ /  | |_) |
    |  __/|  _|  |  _ </ /__ |  __/
    |_|   |_|    |_| \_\____||_|
                    ETL — Adobe Commerce — Magento 2.4.6 — PF B2B [GCP]
    =====================================================================================================
    """
    print(art)
