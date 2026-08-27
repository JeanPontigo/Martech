"""
utils_gcp_ariztia.py — Infraestructura y Utilidades GCP para Ariztía B2B
========================================================================
Responsabilidad: Unificar la autenticación de GCP, logs de control, estados de 
sincronización y la escritura directa del JSON crudo en BigQuery Bronze.

Reemplaza por completo a: bq_client.py y bronze_writer.py
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

log = logging.getLogger("pipeline.utils_gcp_ariztia")

# Constantes globales
DEFAULT_FALLBACK = "2020-01-01 00:00:00"  # BUGFIX (2026-08): estaba en "2026-08-16", casi la fecha actual — impedía que un full-load real trajera histórico completo
TABLE_BRONZE = "bronze.ecommerce"  # Dataset y tabla base en BigQuery
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')

# ---------------------------------------------------------------------------
# 1. CLIENTE BIGQUERY (Autenticación Centralizada)
# ---------------------------------------------------------------------------
def get_bq_client() -> bigquery.Client:
    """
    Crea y devuelve un cliente de BigQuery autenticado.
    Prioridad: 1. GOOGLE_APPLICATION_CREDENTIALS en .env | 2. ADC (Cloud Run/gcloud)
    """
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'), override=True)

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise EnvironmentError(
            "Variable GCP_PROJECT_ID no encontrada en .env\n"
            "Agrega: GCP_PROJECT_ID=martech-data-platform-atlas"
        )

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_path:
        log.debug(f"[bq_client] Autenticando con service account: {credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project_id, credentials=credentials)
    else:
        log.debug("[bq_client] Autenticando con Application Default Credentials (ADC)")
        return bigquery.Client(project=project_id)


def _table_ref(table_name: str) -> str:
    """Helper interno para construir referencias de tablas con backticks en queries SQL."""
    load_dotenv(override=True)
    project = os.getenv("GCP_PROJECT_ID", "martech-data-platform-atlas")
    dataset = os.getenv("BQ_DATASET_BRONZE", "bronze")
    return f"`{project}.{dataset}.{table_name}`"


def ping_bq() -> bool:
    """
    Verifica la conectividad con BigQuery mediante una query mínima.
    Retorna True si es exitosa, False si falla.
    """
    try:
        client = get_bq_client()
        query = "SELECT 1 AS ok"
        result = list(client.query(query).result())
        if result[0]["ok"] == 1:
            log.info("[bq_client] Health check OK")
            return True
    except Exception as e:
        log.error(f"[bq_client] Health check FAILED: {e}")
    return False


# ---------------------------------------------------------------------------
# 2. ESCRITOR BRONZE (Escribe JSON Crudo)
# ---------------------------------------------------------------------------
def write_raw_to_bronze(
    items: list,
    entity: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> bool:
    """
    Escribe una lista de items crudos en bronze.ecommerce de forma Append-only.
    Llamar ANTES del aplanamiento en extract.py. No interrumpe el pipeline si falla.

    Usa batch load (load_table_from_dataframe), no streaming insert —
    consistente con el pipeline de PF. Evita la restricción de streaming
    buffer de BigQuery (~90 min sin poder hacer DELETE/UPDATE sobre filas
    recién insertadas), lo cual facilita iterar y limpiar datos de prueba
    durante desarrollo sin tener que esperar.
    """
    if not items:
        log.info(f"[bronze_writer] entity={entity} → 0 items, nada que escribir")
        return True

    try:
        client = get_bq_client()
        project = client.project
        dataset = os.getenv("BQ_DATASET_BRONZE", "bronze")
        table_id = f"{project}.{dataset}.ecommerce"

        tenant_id = os.getenv("TENANT_ID", "aatn").strip().lower()
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Normalizar ventanas temporales si vienen vacías
        _date_from = date_from or ingested_at
        _date_to = date_to or ingested_at

        rows = [
            {
                "id": str(uuid.uuid4()),
                "tenant_id": tenant_id,
                "entity": entity,
                "raw_json": json.dumps(item, ensure_ascii=False, default=str),
                "date_from": _date_from,
                "date_to": _date_to,
                "ingested_at": ingested_at,
            }
            for item in items
        ]

        df = pd.DataFrame(rows)

        schema = [
            bigquery.SchemaField("id",          "STRING",   mode="REQUIRED"),
            bigquery.SchemaField("tenant_id",   "STRING",   mode="REQUIRED"),
            bigquery.SchemaField("entity",      "STRING",   mode="REQUIRED"),
            bigquery.SchemaField("raw_json",    "STRING",   mode="REQUIRED"),
            bigquery.SchemaField("date_from",   "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("date_to",     "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("ingested_at", "DATETIME", mode="REQUIRED"),
        ]

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        log.info(
            f"[bronze_writer] entity={entity} tenant={tenant_id} "
            f"→ {len(rows):,} filas escritas en {table_id}"
        )
        return True

    except Exception as e:
        log.error(
            f"[bronze_writer] ✗ entity={entity} → excepción inesperada: {e}"
        )
        return False


# Wrappers para simplificar llamadas desde extract.py
def write_orders(items, date_from, date_to, **kwargs) -> bool:
    return write_raw_to_bronze(items, "orders", date_from, date_to)

def write_orders_updated(items, date_from, date_to, **kwargs) -> bool:
    return write_raw_to_bronze(items, "orders_updated", date_from, date_to)

def write_clients(items, date_from=None, date_to=None, **kwargs) -> bool:
    return write_raw_to_bronze(items, "clients", date_from, date_to)

def write_clients_updated(items, date_from=None, date_to=None, **kwargs) -> bool:
    return write_raw_to_bronze(items, "clients_updated", date_from, date_to)

def write_clients_pending(items, **kwargs) -> bool:
    return write_raw_to_bronze(items, "clients_pending")

def write_products(items, date_from=None, date_to=None, **kwargs) -> bool:
    return write_raw_to_bronze(items, "products", date_from, date_to)


# ---------------------------------------------------------------------------
# 3. CONTROL DE ESTADOS Y LOGS (pipeline_state / pipeline_logs)
# ---------------------------------------------------------------------------
def get_last_sync(tenant_id: str, entity: str) -> str:
    """Lee el último checkpoint guardado para delta loading. Fallback si no existe."""
    client = get_bq_client()
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


def update_last_sync(
    tenant_id: str,
    entity: str,
    new_value: str,
    checkpoint_type: str = "datetime",
):
    """Hace un MERGE (upsert) del checkpoint actual en la tabla pipeline_state."""
    client = get_bq_client()
    project = client.project
    dataset = os.getenv("BQ_DATASET_BRONZE", "bronze")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

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
        ON  target.tenant_id = source.tenant_id
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
    """Garantiza la existencia de la tabla interna de control del pipeline."""
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
        pass


def write_log(
    tenant_id: str,
    entity: str,
    status: str,
    records_extracted: int = 0,
    duration_sec: float = 0.0,
    date_from: str = None,
    date_to: str = None,
    error_msg: str = None,
):
    """Registra las métricas e historial de ejecuciones en BigQuery."""
    client = get_bq_client()
    project = client.project
    dataset = os.getenv("BQ_DATASET_BRONZE", "bronze")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
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
        df = pd.DataFrame(rows)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        log.info(
            f"[log] {tenant_id}.{entity} → {status} | "
            f"{records_extracted:,} registros | {duration_sec:.1f}s"
        )
    except Exception as e:
        log.error(f"[log] Error escribiendo pipeline_logs: {e}")


# ---------------------------------------------------------------------------
# 4. HELPERS DE NEGOCIO (Endpoints Magento y Normalizaciones)
# ---------------------------------------------------------------------------
def build_url(endpoint: str) -> str:
    load_dotenv(dotenv_path=_ENV_PATH, override=True)
    base = os.getenv("ADOBE_URL", "https://www.ariztiaatunegocio.cl").rstrip("/")
    return f"{base}/{endpoint.lstrip('/')}"

def build_headers() -> dict:
    if os.path.exists(_ENV_PATH):
        load_dotenv(dotenv_path=_ENV_PATH, override=True)
    token = os.getenv("ADOBE_API_TOKEN", "") or os.getenv("ARIZTIA_API_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


def normalizar_rut(rut) -> str | None:
    """Limpia RUTs chilenos: remueve puntos, añade guion y pasa dígito verificador a minúscula."""
    if pd.isnull(rut):
        return None
    v = str(rut).replace(".", "").lower().strip()
    if not v:
        return None
    if "-" not in v and len(v) > 1:
        v = f"{v[:-1]}-{v[-1]}"
    return v


def ci_art():
    """Genera el banner de inicialización del pipeline en consola."""
    art = r"""
    =====================================================================================================
    ___    ___    _____   _   _
   /   \  /   \    |    | \ | |
  / /\ / / /\ /    |    |  \| |
 / /_// / /_//   _ |    | |\ |
/___/  /___/   |___| \__|_| \_|

                    ETL — Adobe Commerce — Ariztía a tu Negocio [BigQuery]
    =====================================================================================================
    """
    print(art)
