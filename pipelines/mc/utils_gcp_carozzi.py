"""
utils_gcp_carozzi.py — Pipeline GCP Carozzi
============================================
Basado en utils_gcp.py de PF — misma arquitectura, tenant=carozzi
"""

import logging
import os
import uuid
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

log = logging.getLogger("pipeline.utils_gcp_carozzi")

DEFAULT_FALLBACK = "2025-01-01 00:00:00"
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')

# ---------------------------------------------------------------------------
# BigQuery client
# ---------------------------------------------------------------------------
def _get_bq_client() -> bigquery.Client:
    if os.path.exists(_ENV_PATH):
        load_dotenv(dotenv_path=_ENV_PATH, override=True)
    project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    return bigquery.Client(project=project)

def _get_dataset() -> str:
    return os.getenv("BQ_DATASET", "bronze")

def _table_ref(table_name: str) -> str:
    project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset = _get_dataset()
    return f"`{project}.{dataset}.{table_name}`"

# ---------------------------------------------------------------------------
# pipeline_state
# ---------------------------------------------------------------------------
def get_last_sync(tenant_id: str, entity: str) -> str:
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
    client  = _get_bq_client()
    project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset = _get_dataset()
    now     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

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


def _ensure_pipeline_state_table(client, project, dataset):
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
    except Exception:
        pass


# ---------------------------------------------------------------------------
# pipeline_logs
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
    client    = _get_bq_client()
    project   = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset   = _get_dataset()
    now       = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    table_ref = f"{project}.{dataset}.pipeline_logs"

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
    df = pd.DataFrame(rows)

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
# Bronze writer
# ---------------------------------------------------------------------------
def write_raw_to_bronze(
    items:     list,
    entity:    str,
    date_from: str = None,
    date_to:   str = None,
    tenant_id: str = "carozzi",
) -> bool:
    if not items:
        log.info(f"[bronze_writer] entity={entity} → sin registros, omitiendo.")
        return True

    import json
    client   = _get_bq_client()
    project  = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
    dataset  = _get_dataset()
    table_id = f"{project}.{dataset}.ecommerce"
    now      = datetime.utcnow().isoformat()

    rows = [
        {
            "id":          str(uuid.uuid4()),
            "tenant_id":   tenant_id,
            "entity":      entity,
            "raw_json":    json.dumps(item, ensure_ascii=False, default=str),
            "date_from":   date_from or now,
            "date_to":     date_to   or now,
            "ingested_at": now,
        }
        for item in items
    ]

    try:
        BATCH_SIZE = 500
        for i in range(0, len(rows), BATCH_SIZE):
            batch  = rows[i:i + BATCH_SIZE]
            errors = client.insert_rows_json(table=table_id, json_rows=batch)
            if errors:
                log.error(
                    f"[bronze_writer] entity={entity} tenant={tenant_id} "
                    f"→ {len(errors)} errores en lote {i//BATCH_SIZE + 1}:\n{errors[:3]}"
                )
                return False
        log.info(f"[bronze_writer] ✓ entity={entity} → {len(rows):,} filas escritas")
        return True
    except Exception as e:
        log.error(f"[bronze_writer] ✗ entity={entity} → excepción inesperada: {e}")
        return False


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def build_url(endpoint: str) -> str:
    if os.path.exists(_ENV_PATH):
        load_dotenv(dotenv_path=_ENV_PATH, override=True)
    base = os.getenv("CAROZZI_URL", "https://mercadocarozzi.cl").rstrip("/")
    return f"{base}/{endpoint.lstrip('/')}"


def build_headers() -> dict:
    if os.path.exists(_ENV_PATH):
        load_dotenv(dotenv_path=_ENV_PATH, override=True)
    token = os.getenv("CAROZZI_API_TOKEN", "") or os.getenv("MC_TOKEN", "")
    log.info(f"[build_headers] token={'SET' if token else 'EMPTY'}")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


# ---------------------------------------------------------------------------
# ping BigQuery
# ---------------------------------------------------------------------------
def ping_bq() -> bool:
    try:
        client = _get_bq_client()
        client.query("SELECT 1").result()
        return True
    except Exception as e:
        log.error(f"[ping_bq] BigQuery no disponible: {e}")
        return False


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
def ci_czz():
    print("""
    =====================================================================================================
      ____   _   ____    ___   ____  ____  ___
     / ___| / \ |  _ \  / _ \ |_  / |_  / |_ _|
    | |    / _ \| |_) || | | | / /   / /   | |
    | |___ / ___ \  _ < | |_| |/ /__ / /__ | |
     \____|/_/   \_\_| \_\\\\___/|____|____|___|

                    ETL — Magento 2 — Carozzi [BigQuery]
    =====================================================================================================
    """)
