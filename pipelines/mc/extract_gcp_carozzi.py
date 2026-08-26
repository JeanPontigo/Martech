"""
extract_gcp_carozzi.py — Pipeline Carozzi B2B
=============================================
Responsabilidad: obtener datos crudos desde la API de Magento (mercadocarozzi.cl)
y persistirlos en BigQuery Bronze antes de cualquier transformación.

Entidades:
  - orders          → /rest/V1/orders (created_at)
  - orders_updated  → /rest/V1/orders (updated_at)
  - clients         → /rest/V1/customers/search (nuevos por entity_id)
  - clients_updated → /rest/V1/customers/search (actualizados por updated_at)
  - products        → /rest/V1/products
  - categories      → /rest/V1/categories
"""

import json
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Semaphore

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils_gcp_carozzi import build_url, build_headers

log = logging.getLogger("pipeline.extract")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PAGE_SIZE_ORDERS   = 100
PAGE_SIZE_CLIENTS  = 400
PAGE_SIZE_PRODUCTS = 250
MAX_WORKERS        = 2       # Servidor Carozzi sensible a carga paralela
THROTTLE_SLEEP     = 0.2

# ---------------------------------------------------------------------------
# Bronze writer — mismo patrón que PF (load_table_from_dataframe)
# ---------------------------------------------------------------------------
def write_raw_to_bronze(
    items:     list,
    entity:    str,
    date_from: str,
    date_to:   str,
    tenant_id: str = "mc",
):
    if not items:
        return
    try:
        import uuid
        from google.cloud import bigquery
        load_dotenv(override=True)

        project = os.getenv("GCP_PROJECT", "martech-data-platform-atlas")
        dataset = os.getenv("BQ_DATASET", "bronze")
        now     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        rows = [
            {
                "id":          str(uuid.uuid4()),
                "tenant_id":   tenant_id,
                "entity":      entity,
                "raw_json":    json.dumps(item, ensure_ascii=False, default=str),
                "date_from":   date_from,
                "date_to":     date_to,
                "ingested_at": now,
            }
            for item in items
        ]

        df_bq     = pd.DataFrame(rows)
        client    = bigquery.Client(project=project)
        table_ref = f"{project}.{dataset}.ecommerce"

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

        job = client.load_table_from_dataframe(df_bq, table_ref, job_config=job_config)
        job.result()

        log.info(f"[bronze] {len(rows):,} items → {table_ref} | entity={entity} | {date_from} → {date_to}")

    except Exception as e:
        log.error(f"[bronze] Error escribiendo {entity}: {e} — pipeline no afectado")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------
def build_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    headers = build_headers()
    log.info(f"[build_session] token preview: {headers.get('Authorization', 'MISSING')[:30]}")
    session = requests.Session()
    retry   = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS * 2,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update(headers)
    return session


# ---------------------------------------------------------------------------
# Primitivos de paginación
# ---------------------------------------------------------------------------
def _fetch_total_count(session: requests.Session, url_base: str) -> int:
    url  = url_base + "&searchCriteria[pageSize]=1&searchCriteria[currentPage]=1"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("total_count", 0)


def _fetch_page(session: requests.Session, url_base: str, page: int, page_size: int) -> list:
    url  = f"{url_base}&searchCriteria[pageSize]={page_size}&searchCriteria[currentPage]={page}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", data) if isinstance(data, dict) else data


def fetch_all_pages(
    session:     requests.Session,
    url_base:    str,
    page_size:   int,
    label:       str   = "registros",
    max_workers: int   = MAX_WORKERS,
    throttle:    float = THROTTLE_SLEEP,
) -> list:
    total = _fetch_total_count(session, url_base)
    if total == 0:
        log.info(f"[{label}] Sin registros nuevos.")
        return []

    total_pages = -(-total // page_size)
    log.info(
        f"[{label}] {total:,} registros → {total_pages} páginas "
        f"(pageSize={page_size}, workers={max_workers})"
    )

    all_items = []
    semaphore = Semaphore(max_workers)

    def fetch_with_semaphore(page):
        with semaphore:
            return _fetch_page(session, url_base, page, page_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for page in range(1, total_pages + 1):
            futures[executor.submit(fetch_with_semaphore, page)] = page
            time.sleep(throttle)

        for future in as_completed(futures):
            page_num = futures[future]
            try:
                all_items.extend(future.result())
            except Exception as e:
                log.warning(f"[{label}] Error en página {page_num}: {e}")

    log.info(f"[{label}] Extraídos: {len(all_items):,}")
    return all_items


# ---------------------------------------------------------------------------
# SECCIÓN: Órdenes
# ---------------------------------------------------------------------------
def extraer_ordenes(
    session: requests.Session,
    dtFrom:  str,
    dtTo:    str,
) -> pd.DataFrame:
    url_base = build_url(
        "/rest/V1/orders"
        "?searchCriteria[filterGroups][0][filters][0][field]=created_at"
        f"&searchCriteria[filterGroups][0][filters][0][value]={dtFrom}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[filterGroups][1][filters][0][field]=created_at"
        f"&searchCriteria[filterGroups][1][filters][0][value]={dtTo}"
        "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
        "&searchCriteria[sortOrders][0][field]=created_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Órdenes nuevas")
    write_raw_to_bronze(items, entity="orders", date_from=dtFrom, date_to=dtTo)
    log.info(f"[Órdenes nuevas] {len(items):,} órdenes escritas a Bronze.")
    return pd.DataFrame(items) if items else pd.DataFrame()


def extraer_ordenes_actualizadas(
    session: requests.Session,
    dtFrom:  str,
    dtTo:    str,
) -> pd.DataFrame:
    url_base = build_url(
        "/rest/V1/orders"
        "?searchCriteria[filterGroups][0][filters][0][field]=updated_at"
        f"&searchCriteria[filterGroups][0][filters][0][value]={dtFrom}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
        f"&searchCriteria[filterGroups][1][filters][0][value]={dtTo}"
        "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
        "&searchCriteria[sortOrders][0][field]=updated_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Órdenes actualizadas")
    write_raw_to_bronze(items, entity="orders_updated", date_from=dtFrom, date_to=dtTo)
    log.info(f"[Órdenes actualizadas] {len(items):,} escritas a Bronze.")
    return pd.DataFrame(items) if items else pd.DataFrame()


# ---------------------------------------------------------------------------
# SECCIÓN: Clientes
# ---------------------------------------------------------------------------
def extraer_clientes_nuevos(
    session:  requests.Session,
    start_id: int,
) -> pd.DataFrame:
    """Delta por entity_id — igual que PF."""
    url_base = build_url(
        "/rest/V1/customers/search"
        "?searchCriteria[filterGroups][0][filters][0][field]=entity_id"
        f"&searchCriteria[filterGroups][0][filters][0][value]={start_id}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[sortOrders][0][field]=entity_id"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_CLIENTS, label="Clientes nuevos")
    if not items:
        return pd.DataFrame()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="clients", date_from=now, date_to=now)
    log.info(f"[Clientes nuevos] {len(items):,} extraídos.")
    return pd.DataFrame(items)


def extraer_clientes_actualizados(
    session: requests.Session,
    dtFrom:  str,
    dtTo:    str,
) -> pd.DataFrame:
    url_base = build_url(
        "/rest/V1/customers/search"
        "?searchCriteria[filterGroups][0][filters][0][field]=updated_at"
        f"&searchCriteria[filterGroups][0][filters][0][value]={dtFrom}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
        f"&searchCriteria[filterGroups][1][filters][0][value]={dtTo}"
        "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
        "&searchCriteria[sortOrders][0][field]=updated_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_CLIENTS, label="Clientes actualizados")
    if not items:
        return pd.DataFrame()
    write_raw_to_bronze(items, entity="clients_updated", date_from=dtFrom, date_to=dtTo)
    log.info(f"[Clientes actualizados] {len(items):,} extraídos.")
    return pd.DataFrame(items)


# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def extraer_productos(
    session:   requests.Session,
    last_sync: str = None,
) -> pd.DataFrame:
    url_base = build_url(
        "/rest/V1/products"
        "?searchCriteria[sortOrders][0][field]=updated_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    if last_sync:
        url_base += (
            "&searchCriteria[filterGroups][0][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][0][filters][0][value]={last_sync}"
            "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_PRODUCTS, label="Productos")
    if not items:
        return pd.DataFrame()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="products", date_from=last_sync or now, date_to=now)
    log.info(f"[Productos] {len(items):,} escritos a Bronze.")
    return pd.DataFrame(items)


# ---------------------------------------------------------------------------
# SECCIÓN: Categorías
# ---------------------------------------------------------------------------
def _flatten_category(node: dict, level: int = 0) -> list:
    rows = [node]
    for child in node.get("children_data", []):
        rows.extend(_flatten_category(child, level=level + 1))
    return rows


def extraer_categorias(session: requests.Session) -> pd.DataFrame:
    url = build_url("/rest/V1/categories?searchCriteria=20")
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data       = resp.json()
        root_nodes = data.get("items", [data])
        now        = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        write_raw_to_bronze(root_nodes, entity="categories", date_from=now, date_to=now)
        log.info(f"[Categorías] Árbol escrito a Bronze.")
        return pd.DataFrame(root_nodes)
    except requests.RequestException as e:
        log.error(f"[Categorías] Error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# SECCIÓN: Mappings auxiliares
# ---------------------------------------------------------------------------
def get_customer_group_mapping(session: requests.Session) -> dict:
    url = build_url("/rest/V1/customerGroups/search?searchCriteria=[]")
    try:
        resp   = session.get(url, timeout=15)
        resp.raise_for_status()
        grupos = resp.json().get("items", [])
        mapping = {g["id"]: g["code"] for g in grupos}
        log.info(f"[Grupos cliente] {len(mapping)} grupos cargados.")
        return mapping
    except requests.RequestException as e:
        log.warning(f"[Grupos cliente] No se pudo obtener mapping: {e}")
        return {}
