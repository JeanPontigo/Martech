"""
extract.py  — Pipeline PF B2B
==============================
Responsabilidad: obtener datos crudos desde la API de Magento (tiendapfalimentos.cl).

Estándar aplicado (patrón Ariztia):
- HTTP Session con connection pooling + retry/backoff exponencial
- total_count en UN solo request (sin requests duplicados)
- Paginación paralela con ThreadPoolExecutor + throttle via Semaphore
- Delta loading por updated_at / entity_id en todas las entidades
- Logging estructurado (sin print, sin tqdm, sin pqdm)
- Eliminadas dependencias de pqdm y tqdm
- write_raw_to_bronze: persiste JSON crudo en BigQuery Bronze antes del transform
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

from utils_gcp import build_url, build_headers

log = logging.getLogger("pipeline.extract")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PAGE_SIZE_ORDERS    = 100
PAGE_SIZE_CLIENTS   = 100
PAGE_SIZE_PRODUCTS  = 100
PAGE_SIZE_COMPANIES = 500
MAX_WORKERS         = 4
THROTTLE_SLEEP      = 0.2

ESTADO_COMPANY = {0: "PENDING", 1: "ACTIVE", 2: "REJECTED", 3: "BLOCKED"}

# ---------------------------------------------------------------------------
# BigQuery Bronze — escritura de datos crudos (JSON)
# ---------------------------------------------------------------------------
def write_raw_to_bronze(items: list, entity: str, date_from: str, date_to: str, tenant_id: str = "pf"):
    """
    Persiste la lista de items crudos (JSON de la API) en bronze.ecommerce.

    Schema:
      id          → UUID v4 — PK
      tenant_id   → identificador del cliente
      entity      → tipo de entidad: orders, clients, products, etc.
      raw_json    → payload completo tal como viene de la API
      date_from   → inicio del rango de extracción (dtFrom del pipeline)
      date_to     → fin del rango de extracción (dtTo del pipeline)
      ingested_at → timestamp UTC de ingesta

    Mapeo entity por fuente:
      orders          → /rest/V1/orders (created_at)
      orders_updated  → /rest/V1/orders (updated_at)
      clients         → /rest/V1/customers/search (nuevos)
      clients_updated → /rest/V1/customers/search (actualizados)
      products        → /rest/V1/products
      categories      → /rest/V1/categories
      companies       → /rest/V1/amcompany/company/search

    No interrumpe el pipeline si falla — solo loguea el error.
    """
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
# HTTP Session compartida — connection pooling + retry exponencial
# ---------------------------------------------------------------------------
def build_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """
    Session reutilizable:
    - Pool de conexiones TCP (evita handshake por cada request)
    - Retry automático en 429/500/502/503/504
      delay = backoff * (2 ^ intento)  →  1s, 2s, 4s ...
    """
    session = requests.Session()
    retry = Retry(
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
    session.mount("http://", adapter)
    session.headers.update(build_headers())
    return session

# ---------------------------------------------------------------------------
# Primitivos de paginación genérica
# ---------------------------------------------------------------------------
def _fetch_total_count(session: requests.Session, url_base: str) -> int:
    """Un solo request para obtener total_count — sin duplicar calls."""
    url = url_base + "&searchCriteria[pageSize]=1&searchCriteria[currentPage]=1"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("total_count", 0)

def _fetch_page(session: requests.Session, url_base: str, page: int, page_size: int) -> list:
    url = f"{url_base}&searchCriteria[pageSize]={page_size}&searchCriteria[currentPage]={page}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", data) if isinstance(data, dict) else data

def fetch_all_pages(
    session: requests.Session,
    url_base: str,
    page_size: int,
    label: str = "registros",
    max_workers: int = MAX_WORKERS,
    throttle: float = THROTTLE_SLEEP,
) -> list:
    """
    Paginación paralela genérica con throttle controlado:
    1. Un solo request → total_count → calcula total_pages exacto
    2. Semáforo limita concurrencia (protege el backend)
    3. Sleep entre lanzamientos (throttle fino)
    4. Retry implícito vía Session
    """
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
def _aplanar_ordenes(items: list) -> pd.DataFrame:
    """
    Aplana campos anidados de /rest/V1/orders.
    Expande items[] de cada orden en filas individuales (una por line item),
    preservando todos los campos de cabecera en cada fila.
    """
    if not items:
        return pd.DataFrame()

    rows = []
    for orden in items:
        ext  = orden.get("extension_attributes") or {}
        comp = ext.get("amcompany_attributes") or {}

        # Dirección de envío (shipping_assignments)
        ciudad_envio = None
        try:
            ciudad_envio = (
                ext["shipping_assignments"][0]["shipping"]["address"]["city"]
            )
        except (KeyError, IndexError, TypeError):
            pass

        # Cabecera de la orden
        cabecera = {
            "order_id":       orden.get("entity_id"),
            "fecha":          orden.get("created_at"),
            "updated_at":     orden.get("updated_at"),
            "fecha_envio":    ext.get("reparto_date"),
            "ciudad_envio":   ciudad_envio,
            "estado":         orden.get("status"),
            "total":          orden.get("grand_total", 0),
            "subtotal":       orden.get("subtotal", 0),
            "descuento":      orden.get("discount_amount", 0),
            "envio":          orden.get("shipping_amount", 0),
            "client_id":      orden.get("customer_id"),
            "cliente_email":  orden.get("customer_email"),
            "group_id":       orden.get("customer_group_id"),
            "ciudad":         (orden.get("billing_address") or {}).get("city"),
            "region":         (orden.get("billing_address") or {}).get("region"),
            "payment_method": (orden.get("payment") or {}).get("method"),
            "coupon_code":    ext.get("coupon_code"),
            "company_id":     comp.get("company_id"),
            "company_name":   comp.get("company_name"),
        }

        for item in orden.get("items", []):
            row = {
                **cabecera,
                "sku":             item.get("sku"),
                "sku_name":        item.get("name"),
                "precio_unitario": item.get("price", 0),
                "base_price":      item.get("base_price", 0),
                "original_price":  item.get("original_price", 0),
                "cantidad":        item.get("qty_ordered", 0),
                "total_linea":     item.get("row_total", 0),
            }
            rows.append(row)

    return pd.DataFrame(rows)


def extraer_ordenes(session: requests.Session, dtFrom: str, dtTo: str, write_bronze: bool = True) -> pd.DataFrame:
    """
    Extrae órdenes NUEVAS filtrando por created_at.
    Usado para INSERT.
    """
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
    if write_bronze:
        write_raw_to_bronze(items, entity="orders", date_from=dtFrom, date_to=dtTo)
    df = _aplanar_ordenes(items)
    log.info(f"[Órdenes nuevas] {len(df):,} filas ({df['order_id'].nunique() if not df.empty else 0} órdenes).")
    return df


def extraer_ordenes_actualizadas(session: requests.Session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Extrae órdenes MODIFICADAS filtrando por updated_at.
    Usado para UPDATE de estado.
    """
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
    df = _aplanar_ordenes(items)
    log.info(f"[Órdenes actualizadas] {len(df):,} filas extraídas.")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes
# ---------------------------------------------------------------------------
GRUPOS_CONOCIDOS_PF = {0, 1, 14, 15, 16}
def _parse_cliente(c: dict) -> dict:
    """Extrae campos planos + custom_attributes de un registro de cliente."""
    rut = None
    company_rut = None
    for attr in c.get("custom_attributes", []):
        code = attr.get("attribute_code")
        if code == "rut":
            rut = attr.get("value")
        elif code == "rp_company_rut":
            company_rut = attr.get("value")
    return {
        "id":           c.get("id"),
        "group_id":     c.get("group_id"),
        "created_at":   c.get("created_at"),
        "updated_at":   c.get("updated_at"),
        "email":        c.get("email"),
        "firstname":    c.get("firstname"),
        "lastname":     c.get("lastname"),
        "rut":          rut,
        "company_rut":  company_rut,
        "company_id":   (c.get("extension_attributes") or {}).get("company_id"),
    }


def extraer_clientes_nuevos(session: requests.Session, start_id: int) -> pd.DataFrame:
    """
    Extrae clientes con entity_id > start_id (delta por ID).
    Estrategia: filtro gteq sobre entity_id + paginación paralela.
    """
    url_base = build_url(
        "/rest/V1/customers/search"
        f"?searchCriteria[filterGroups][0][filters][0][field]=entity_id"
        f"&searchCriteria[filterGroups][0][filters][0][value]={start_id}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[sortOrders][0][field]=entity_id"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_CLIENTS, label="Clientes nuevos")
    if not items:
        return pd.DataFrame()

    for cliente in items:
        group_id = cliente.get("group_id")
        if group_id not in GRUPOS_CONOCIDOS_PF:
            log.warning(
                f"[Clientes nuevos] Nuevo grupo detectado — "
                f"group_id={group_id} | "
                f"client_id={cliente.get('id')} | "
                f"email={cliente.get('email')}"
            )

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="clients", date_from=now, date_to=now)
    df = pd.DataFrame([_parse_cliente(c) for c in items])
    log.info(f"[Clientes nuevos] {len(df):,} clientes extraídos.")
    return df


def extraer_clientes_actualizados(session: requests.Session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Extrae clientes modificados en el rango updated_at.
    Usado para actualizar datos de clientes existentes.
    """
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
    
    for cliente in items:
        group_id = cliente.get("group_id")
        if group_id not in GRUPOS_CONOCIDOS_PF:
            log.warning(
                f"[Clientes actualizados] Nuevo grupo detectado — "
                f"group_id={group_id} | "
                f"client_id={cliente.get('id')} | "
                f"email={cliente.get('email')}"
            )
                
    write_raw_to_bronze(items, entity="clients_updated", date_from=dtFrom, date_to=dtTo)
    df = pd.DataFrame([_parse_cliente(c) for c in items])
    log.info(f"[Clientes actualizados] {len(df):,} clientes extraídos.")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def extraer_productos(session: requests.Session, last_sync: str = None) -> pd.DataFrame:
    """
    Extrae productos. Con last_sync filtra solo actualizados desde ese timestamp
    (delta loading — en corridas normales extrae muy poco).
    """
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

    rows = []
    for item in items:
        custom = {
            a["attribute_code"]: a.get("value")
            for a in item.get("custom_attributes", [])
        }
        rows.append({
            "id":               item.get("id"),
            "sku":              item.get("sku"),
            "sku_name":         item.get("name"),
            "product_type":     item.get("type_id"),
            "availability":     item.get("status"),
            "created_at":       item.get("created_at"),
            "updated_at":       item.get("updated_at"),
            "peso_promedio_kg": custom.get("peso_promedio"),
            "unidad_x_producto":custom.get("unidad_x_producto"),
            "marca_logo":       str(custom.get("marca_logo") or ""),
            "category_id":      custom.get("category_ids"),
            "image":            custom.get("image"),
        })

    df = pd.DataFrame(rows)
    log.info(f"[Productos] {len(df):,} productos extraídos.")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Categorías
# ---------------------------------------------------------------------------
def _flatten_category(node: dict, parent_path: str = "", level: int = 0) -> list:
    path = f"{parent_path}/{node.get('name')}".lstrip("/")
    rows = [{
        "category_id":   node.get("id"),
        "parent_id":     node.get("parent_id"),
        "category_name": node.get("name"),
        "is_active":     node.get("is_active"),
        "product_count": node.get("product_count"),
        "level":         level,
        "path":          path,
    }]
    for child in node.get("children_data", []):
        rows.extend(_flatten_category(child, parent_path=path, level=level + 1))
    return rows


def extraer_categorias(session: requests.Session) -> pd.DataFrame:
    """
    Extrae el árbol completo de categorías (endpoint no paginado).
    Las categorías cambian raramente — se extrae siempre completo.
    """
    url = build_url("/rest/V1/categories?searchCriteria=20")
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        root_nodes = data.get("items", [data])
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        write_raw_to_bronze(root_nodes, entity="categories", date_from=now, date_to=now)
        filas = []
        for nodo in root_nodes:
            filas.extend(_flatten_category(nodo))
        df = pd.DataFrame(filas)[[
            "category_id", "parent_id", "category_name",
            "is_active", "product_count", "level", "path"
        ]]
        log.info(f"[Categorías] {len(df):,} registros extraídos.")
        return df
    except requests.RequestException as e:
        log.error(f"[Categorías] Error: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# SECCIÓN: Compañías
# ---------------------------------------------------------------------------
def _parse_companias(items: list) -> pd.DataFrame:
    """Normaliza una lista de items del endpoint de compañías a DataFrame."""
    rows = []
    for item in items:
        rows.append({
            "company_id":     item.get("company_id"),
            "company_name":   item.get("company_name"),
            "status":         ESTADO_COMPANY.get(item.get("status")),
            "company_email":  item.get("company_email"),
            "city":           item.get("city"),
            "region":         item.get("region"),
            "rut_company":    item.get("rut_company"),
            "oficina_venta":  item.get("codigo_stock"),
            "codigo_oficina": item.get("codigo_oficina"),
            "company_code":   item.get("codigo_cliente"),
        })
    return pd.DataFrame(rows).drop_duplicates("company_id") if rows else pd.DataFrame()


def _fetch_companias_secuencial(session: requests.Session, url_base: str, label: str) -> list:
    """
    Paginación secuencial para el endpoint de Amasty.
    No acepta paginación paralela — se itera página a página.
    Termina cuando la respuesta devuelve menos ítems que el pageSize.
    """
    PAGE_SIZE = 5000
    page      = 1
    all_items = []

    while True:
        url = f"{url_base}&searchCriteria[currentPage]={page}"
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            items = resp.json().get("items", [])

            if not items:
                break

            all_items.extend(items)
            log.info(f"[{label}] Página {page}: {len(items)} registros (acumulado: {len(all_items)})")

            if len(items) < PAGE_SIZE:
                break

            page += 1

        except requests.RequestException as e:
            log.warning(f"[{label}] Error en página {page}: {e}")
            break

    return all_items


def extraer_companias_nuevas(session: requests.Session, start_id: int) -> pd.DataFrame:
    """
    Extrae compañías con company_id > start_id (delta por ID).
    Equivalente a extraer_clientes_nuevos — mismo patrón.
    Corrida diaria: solo trae las compañías registradas desde la última sync.
    """
    if start_id == 0:
        log.info("[Compañías nuevas] Base vacía — delegando a full load.")
        return extraer_companias(session)

    url_base = build_url(
        "/rest/V1/amcompany/company/search"
        "?searchCriteria[filterGroups][0][filters][0][field]=company_id"
        f"&searchCriteria[filterGroups][0][filters][0][value]={start_id}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gt"
        "&searchCriteria[sortOrders][0][field]=company_id"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = _fetch_companias_secuencial(session, url_base, label="Compañías nuevas")
    if not items:
        log.info("[Compañías nuevas] Sin compañías nuevas.")
        return pd.DataFrame()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="companies", date_from=now, date_to=now)
    df = _parse_companias(items)
    log.info(f"[Compañías nuevas] {len(df):,} compañías extraídas.")
    return df


def extraer_companias(session: requests.Session) -> pd.DataFrame:
    """
    Full load de todas las compañías — sin filtro de ID.
    Uso: primera carga (--full-load) o recuperación manual.
    En corridas normales se usa extraer_companias_nuevas().
    """
    url_base = build_url("/rest/V1/amcompany/company/search?searchCriteria=all")
    items = _fetch_companias_secuencial(session, url_base, label="Compañías full")
    if not items:
        log.info("[Compañías full] Sin registros extraídos.")
        return pd.DataFrame()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="companies", date_from=now, date_to=now)
    df = _parse_companias(items)
    log.info(f"[Compañías full] {len(df):,} compañías extraídas.")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Company Access
# ---------------------------------------------------------------------------
def extraer_company_access(session: requests.Session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Extrae accesos de contacto por sucursal desde endpoint custom martech.
    Delta por created_at. El campo 'password' se sanitiza antes de escribir a Bronze.
    """
    url_base = build_url(
        "/rest/V1/martech-extcompanyusers/access/search"
        "?searchCriteria[filterGroups][0][filters][0][field]=created_at"
        f"&searchCriteria[filterGroups][0][filters][0][value]={dtFrom}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[filterGroups][1][filters][0][field]=created_at"
        f"&searchCriteria[filterGroups][1][filters][0][value]={dtTo}"
        "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
        "&searchCriteria[sortOrders][0][field]=created_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_CLIENTS, label="Company Access")
    if not items:
        return pd.DataFrame()

    # Sanitizar campo password antes de escribir a Bronze
    for item in items:
        item.pop("password", None)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="company_access", date_from=dtFrom, date_to=dtTo)
    df = pd.DataFrame(items)
    log.info(f"[Company Access] {len(df):,} accesos extraídos.")
    return df


def extraer_company_access_full(session: requests.Session) -> pd.DataFrame:
    """
    Full load de todos los accesos de company_access.
    Uso: primera carga o recuperación manual.
    """
    url_base = build_url(
        "/rest/V1/martech-extcompanyusers/access/search"
        "?searchCriteria[sortOrders][0][field]=created_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_CLIENTS, label="Company Access full")
    if not items:
        return pd.DataFrame()

    # Sanitizar campo password antes de escribir a Bronze
    for item in items:
        item.pop("password", None)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, entity="company_access", date_from=now, date_to=now)
    df = pd.DataFrame(items)
    log.info(f"[Company Access full] {len(df):,} accesos extraídos.")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Mappings auxiliares (marcas, grupos de cliente)
# — se consultan una sola vez y se cachean en memoria dentro de la sesión ETL
# ---------------------------------------------------------------------------
def get_brand_mapping(session: requests.Session) -> dict:
    """Devuelve {marca_logo_id: nombre_marca} desde el endpoint de atributos."""
    url = build_url("/rest/V1/products/attributes/marca_logo/options")
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        options = resp.json()
        mapping = {
            str(opt["value"]): opt["label"].strip()
            for opt in options if opt.get("value")
        }
        log.info(f"[Marcas] {len(mapping)} marcas cargadas.")
        return mapping
    except requests.RequestException as e:
        log.warning(f"[Marcas] No se pudo obtener mapping: {e}")
        return {}


def get_customer_group_mapping(session: requests.Session) -> dict:
    """Devuelve {group_id: nombre_grupo} desde el endpoint de grupos."""
    url = build_url("/rest/V1/customerGroups/search?searchCriteria=[]")
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        grupos = resp.json().get("items", [])
        mapping = {g["id"]: g["code"] for g in grupos}
        log.info(f"[Grupos cliente] {len(mapping)} grupos cargados.")
        return mapping
    except requests.RequestException as e:
        log.warning(f"[Grupos cliente] No se pudo obtener mapping: {e}")
        return {}
