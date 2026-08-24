"""
extract.py
==========
Responsabilidad: obtener datos crudos desde la API de Magento.

Fuentes:
- Orders INSERT + Grid  → endpoint custom (UTC-4, centro, rut real, sin sublocales)
- Orders UPDATE status  → endpoint nativo /rest/V1/orders (updated_at, UTC)
- Clientes, Productos, Pendientes → sin cambios

Características:
- HTTP Session con connection pooling + retry/backoff exponencial
- Paginación paralela (nativo) y secuencial (custom — lista directa sin total_count)
- Conversión UTC-4 → UTC solo para el UPDATE de status (offset fijo -4h)
- Logging estructurado (sin print)
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from threading import Semaphore

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import build_url, build_headers

log = logging.getLogger("pipeline.extract")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PAGE_SIZE_ORDERS   = 100
PAGE_SIZE_CLIENTS  = 100
PAGE_SIZE_PRODUCTS = 200
MAX_WORKERS        = 8
THROTTLE_SLEEP     = 0.2

# Offset fijo UTC-4 (Chile invierno). Verano es UTC-3 → 1h de diferencia aceptada.
UTC_OFFSET  = timedelta(hours=4)
UTC_MINUS_4 = timezone(-UTC_OFFSET)

# Endpoint custom — fuente principal de orders y grid
CUSTOM_URL   = "https://www.ariztiaatunegocio.cl/rest/V1/martech-ariztiacustomers/orders/search"
CUSTOM_TOKEN = "n1edbnizhepyliq34y4k437u7mglz3my"

# ---------------------------------------------------------------------------
# HTTP Session compartida — connection pooling + retry con backoff exponencial
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
# Primitivos de paginación — para el endpoint nativo
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

    total_pages = -(-total // page_size)  # ceil sin math
    log.info(f"[{label}] {total:,} registros → {total_pages} páginas (pageSize={page_size}, workers={max_workers})")

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
                items = future.result()
                all_items.extend(items)
            except Exception as e:
                log.warning(f"[{label}] Error en página {page_num}: {e}")

    log.info(f"[{label}] Extraídos: {len(all_items):,}")
    return all_items

# ---------------------------------------------------------------------------
# SECCIÓN: Orders — extracción desde endpoint custom
# ---------------------------------------------------------------------------
def extraer_ordenes_custom(
    session: requests.Session,
    dtFrom: str = None,
    dtTo:   str = None,
) -> pd.DataFrame:
    """
    Extrae órdenes + items desde el endpoint custom (UTC-4).

    - Paginación secuencial (lista directa, sin total_count).
    - Filtro opcional por fecha_compra. Sin filtro → full-load completo.
    - Cada fila es un item de orden (una orden tiene N filas).
    - Campos clave: sap_id, order_id (increment_id con ceros), customer_sap_id,
      company_id, centro, rut_company, fecha_compra, hora_compra, fecha_despacho,
      sku, price, cantidad, venta_neta, venta_bruta, marca, etc.

    El DataFrame resultante se usa tanto para orders como para grid:
        transform_orders → agrupa por order_id → ariztia_orders  (INSERT)
        transform_grid   → usa filas directamente → ariztia_grid  (INSERT)
    """
    PAGE_SIZE = 100
    page      = 1
    all_items = []
    headers   = {"Authorization": f"Bearer {CUSTOM_TOKEN}"}

    while True:
        params = {
            "searchCriteria[pageSize]":    PAGE_SIZE,
            "searchCriteria[currentPage]": page,
        }
        if dtFrom:
            params["searchCriteria[filterGroups][0][filters][0][field]"]          = "fecha_compra"
            params["searchCriteria[filterGroups][0][filters][0][value]"]          = dtFrom
            params["searchCriteria[filterGroups][0][filters][0][condition_type]"] = "gteq"
        if dtTo:
            params["searchCriteria[filterGroups][1][filters][0][field]"]          = "fecha_compra"
            params["searchCriteria[filterGroups][1][filters][0][value]"]          = dtTo
            params["searchCriteria[filterGroups][1][filters][0][condition_type]"] = "lteq"

        resp = session.get(CUSTOM_URL, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        data  = resp.json()
        items = data if isinstance(data, list) else data.get("items", [])

        if not items:
            break

        all_items.extend(items)
        log.info(f"[Orders custom] Página {page}: {len(items)} filas | Acumulado: {len(all_items):,}")

        if len(items) < PAGE_SIZE:
            break

        page += 1

    log.info(f"[Orders custom] Total extraídos: {len(all_items):,} filas")
    return pd.DataFrame(all_items) if all_items else pd.DataFrame()


def _local_to_utc(dt_str: str) -> str:
    """
    Convierte fecha string UTC-4 → UTC para consultar el endpoint nativo.
    Offset fijo -4h. En verano (UTC-3) hay 1h de diferencia — aceptada.
    """
    dt_local = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC_MINUS_4)
    return dt_local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _aplanar_ordenes_nativo(items: list) -> pd.DataFrame:
    """
    Aplana campos de /rest/V1/orders.
    Solo se usa para extraer_ordenes_actualizadas (UPDATE de status).
    Extrae únicamente: entity_id, increment_id, status.
    """
    if not items:
        return pd.DataFrame()
    rows = []
    for order in items:
        ext = order.get("extension_attributes") or {}
        rows.append({
            "entity_id":    order.get("entity_id"),
            "increment_id": order.get("increment_id"),
            "status":       order.get("status"),
            "updated_at":   order.get("updated_at"),
            "sap_id":       ext.get("sap_id"),
        })
    return pd.DataFrame(rows)


def extraer_ordenes_actualizadas(
    session: requests.Session,
    dtFrom:  str,
    dtTo:    str,
) -> pd.DataFrame:
    """
    Extrae órdenes MODIFICADAS desde el nativo filtrando por updated_at.
    Se usa exclusivamente para UPDATE de status en ariztia_orders.

    dtFrom / dtTo vienen en UTC-4 (hora Chile) → se convierten a UTC
    antes de consultar el endpoint nativo (que trabaja en UTC).
    """
    dtFrom_utc = _local_to_utc(dtFrom)
    dtTo_utc   = _local_to_utc(dtTo)

    log.info(f"[Orders actualizadas] Consultando nativo UTC: {dtFrom_utc} → {dtTo_utc}")

    url_base = build_url(
        "/rest/V1/orders"
        "?searchCriteria[filterGroups][0][filters][0][field]=updated_at"
        f"&searchCriteria[filterGroups][0][filters][0][value]={dtFrom_utc}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        "&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
        f"&searchCriteria[filterGroups][1][filters][0][value]={dtTo_utc}"
        "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
        "&searchCriteria[sortOrders][0][field]=updated_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Orders actualizadas")
    df    = _aplanar_ordenes_nativo(items)
    log.info(f"[Orders actualizadas] {len(df):,} órdenes extraídas.")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Aprobados
# ---------------------------------------------------------------------------
def extraer_clientes_aprobados(session: requests.Session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """Extrae clientes aprobados en rango de fechas."""
    url_base = build_url(
        "/rest/V1/martech-ariztiacustomers/access/search"
        "?searchCriteria[filterGroups][0][filters][0][field]=created_at"
        f"&searchCriteria[filterGroups][0][filters][0][value]={dtFrom}"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=from"
        "&searchCriteria[filterGroups][1][filters][0][field]=created_at"
        f"&searchCriteria[filterGroups][1][filters][0][value]={dtTo}"
        "&searchCriteria[filterGroups][1][filters][0][condition_type]=to"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_CLIENTS, label="Clientes aprobados")
    if not items:
        return pd.DataFrame()

    return pd.DataFrame(items, columns=[
        "entity_id", "adobe_id", "sap_id", "rut_company", "centro", "status",
        "razon_social", "contacto", "celular", "email", "created_at", "last_login", "region"
    ]).rename(columns={"sap_id": "id_sap", "rut_company": "usu_rut"})

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
    return pd.DataFrame(items) if items else pd.DataFrame()

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Pendientes
# ---------------------------------------------------------------------------
def extraer_clientes_pendientes(session: requests.Session) -> pd.DataFrame:
    """
    Paginación secuencial — el endpoint devuelve lista directa sin total_count.
    """
    PAGE_SIZE = 500
    page      = 1
    all_items = []

    while True:
        url = build_url(
            "/rest/V1/martech-ariztiacustomers/pending/search"
            f"?searchCriteria[pageSize]={PAGE_SIZE}"
            f"&searchCriteria[currentPage]={page}"
            "&searchCriteria[sortOrders][0][field]=id_adobe"
            "&searchCriteria[sortOrders][0][direction]=ASC"
        )
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data  = resp.json()
        items = data if isinstance(data, list) else data.get("items", [])

        if not items:
            break

        all_items.extend(items)
        log.info(f"[Clientes pendientes] Página {page}: {len(items)} registros")

        if len(items) < PAGE_SIZE:
            break

        page += 1

    log.info(f"[Clientes pendientes] Total extraídos: {len(all_items):,}")
    return pd.DataFrame(all_items) if all_items else pd.DataFrame()
