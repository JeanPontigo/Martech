"""
extract_gcp_ariztia.py  — Pipeline Ariztia B2B
====================================
Responsabilidad: obtener datos crudos desde la API de Magento/Adobe Commerce
(ariztiaatunegocio.cl) y persistirlos en BigQuery Bronze antes de cualquier
transformación.

Fuentes:
  - Orders INSERT     → 100% endpoint custom (fuente única — cabecera + items)
  - Orders UPDATE     → endpoint nativo /rest/V1/orders (updated_at) — solo status
  - Clientes aprobados → /rest/V1/martech-ariztiacustomers/access/search
  - Clientes pendientes → /rest/V1/martech-ariztiacustomers/pending/search
  - Productos          → /rest/V1/products

Estrategia de orders (validada 2026-08 — consistente con pipeline legacy MySQL,
ver ariztia_orders + ariztia_grid):
  - El custom entrega una lista PLANA: una fila por línea de orden, repitiendo
    los campos de cabecera en cada línea (status, company_id, customer_sap_id,
    razon_social, centro, comuna, fecha_compra, etc.)
  - Se agrupa por order_id para reconstruir orden + items[] antes de Bronze
  - El nativo NO se usa para el INSERT — no representa fielmente todas las
    líneas reales de una orden en Ariztía (caso validado: orden 237972,
    nativo trae 3 SKUs, custom/grid trae 11 SKUs reales)
  - El nativo se sigue usando exclusivamente para detectar cambios de status
    vía filtro updated_at — el custom no soporta ese tipo de filtro incremental

Mapeo de identidad (bridge cliente/empresa — consistente con silver.fact_orders):
  - client_id  ← customer_sap_id
  - company_id ← company_id
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Semaphore

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils_gcp_ariztia import (
    build_url,
    build_headers,
    write_raw_to_bronze,
)

log = logging.getLogger("pipeline.extract")

load_dotenv()

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PAGE_SIZE_ORDERS   = 100
PAGE_SIZE_CLIENTS  = 100
PAGE_SIZE_PRODUCTS = 200
MAX_WORKERS        = 4
THROTTLE_SLEEP     = 0.2

CUSTOM_URL   = "https://www.ariztiaatunegocio.cl/rest/V1/martech-ariztiacustomers/orders/search"
CUSTOM_TOKEN = "n1edbnizhepyliq34y4k437u7mglz3my"

COMPANY_URL  = "https://www.ariztiaatunegocio.cl/rest/V1/company"


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------
def build_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
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
    session.headers.update(build_headers())
    return session


# ---------------------------------------------------------------------------
# Primitivos de paginación — endpoints nativos
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
# Parseo defensivo de JSON — el endpoint custom de Ariztía puede devolver
# secuencias de escape inválidas dentro de strings (ej. "Champi\ñones",
# "Jam\ón" — un backslash literal antes de una vocal acentuada/ñ, que no es
# un escape JSON válido). json.loads() falla duro ante esto (JSONDecodeError:
# Invalid \escape). Se limpia el texto antes de parsear como fallback,
# eliminando backslashes que no preceden a un escape JSON válido
# (" \ / b f n r t u). No afecta payloads bien formados — solo actúa
# cuando el parseo estándar falla.
# ---------------------------------------------------------------------------
import re as _re
import json as _json

_INVALID_ESCAPE_RE = _re.compile(r'\\(?!["\\/bfnrtu])')


def _parse_json_response(resp: requests.Response, label: str = ""):
    """
    Intenta resp.json() normal. Si falla por escape inválido, reintenta
    limpiando backslashes sueltos del texto crudo antes de parsear.
    """
    try:
        return resp.json()
    except ValueError as e:
        log.warning(
            f"[{label}] JSON inválido en respuesta cruda — "
            f"reintentando con limpieza de escapes ({e})"
        )
        cleaned = _INVALID_ESCAPE_RE.sub("", resp.text)
        return _json.loads(cleaned)


# ---------------------------------------------------------------------------
# Paginación secuencial — endpoint custom Ariztia
# ---------------------------------------------------------------------------
def _fetch_custom_secuencial(
    params:    dict,
    label:     str = "custom",
    page_size: int = 100,
) -> list:
    page      = 1
    all_items = []
    headers   = {"Authorization": f"Bearer {CUSTOM_TOKEN}"}
    session   = requests.Session()

    while True:
        params_page = {
            **params,
            "searchCriteria[pageSize]":    page_size,
            "searchCriteria[currentPage]": page,
        }
        resp  = session.get(CUSTOM_URL, params=params_page, headers=headers, timeout=60)
        resp.raise_for_status()
        data  = _parse_json_response(resp, label=label)
        items = data if isinstance(data, list) else data.get("items", [])

        if not items:
            break

        all_items.extend(items)
        log.info(f"[{label}] Página {page}: {len(items)} filas | Acumulado: {len(all_items):,}")

        if len(items) < page_size:
            break

        page += 1

    log.info(f"[{label}] Total extraídos: {len(all_items):,} filas")
    return all_items


# ---------------------------------------------------------------------------
# Construcción de orden desde custom — fuente única de verdad para el INSERT
# ---------------------------------------------------------------------------
# El endpoint custom entrega una lista PLANA: una fila por línea de orden,
# repitiendo los campos de cabecera en cada línea. Se agrupa por order_id
# para reconstruir la estructura orden + items[], igual que el pipeline
# legacy MySQL (ariztia_orders + ariztia_grid, ambos derivados del custom).
#
# client_id  → customer_sap_id  (resuelve adjudicación compra→cliente)
# company_id → company_id       (bridge a dim_company, mismo campo que PF)
# ---------------------------------------------------------------------------

# Campos de CABECERA — idénticos en todas las líneas de una misma orden
_HEADER_FIELDS = [
    "order_id", "status", "sap_id", "fecha_compra", "hora_compra",
    "fecha_despacho", "email", "company_id", "customer_sap_id",
    "razon_social", "telephone", "rut_company", "direccion",
    "centro", "comuna", "iva", "tipo_cliente",
]

# Campos de LÍNEA — propios de cada item dentro de la orden
_LINE_FIELDS = [
    "item_id", "sku", "name", "price", "cantidad", "venta_neta",
    "descuento_promo", "id_descuento", "porcentaje_descuento",
    "monto_descuento", "venta_bruta", "valor_envio", "total",
    "total_final", "nombre_cupon", "product_entity_id", "row_id",
    "marca_id", "marca", "id_category", "venta_bruta_custom_tax",
    "total_custom_tax", "total_final_custom_tax", "main_cat_row_id",
    "category_name", "id_child_category", "sub_cat_row_id",
    "child_category_name", "price_unit", "sales_unit",
]


def _construir_ordenes_desde_custom(items_custom: list) -> list:
    """
    Agrupa las filas planas del custom por order_id y reconstruye
    la estructura orden + items[].
    Cabecera se toma de la primera línea de cada grupo (son idénticas
    en todas las líneas de esa orden); items[] preserva cada línea completa.
    """
    grouped: dict = {}
    for row in items_custom:
        order_id = str(row.get("order_id", ""))
        grouped.setdefault(order_id, []).append(row)

    orders = []
    for order_id, rows in grouped.items():
        header = {field: rows[0].get(field) for field in _HEADER_FIELDS}
        header["items"] = [
            {field: row.get(field) for field in _LINE_FIELDS}
            for row in rows
        ]
        orders.append(header)

    return orders


# ---------------------------------------------------------------------------
# SECCIÓN: Orders — 100% desde endpoint custom (INSERT)
# ---------------------------------------------------------------------------
def extraer_ordenes_custom(
    session: requests.Session,
    dtFrom:  str,
    dtTo:    str,
) -> pd.DataFrame:
    """
    Extrae órdenes completas (cabecera + items) desde el endpoint custom —
    fuente de verdad única, consistente con el pipeline legacy MySQL.
    El parámetro `session` se conserva por compatibilidad de firma con
    main_gcp_ariztia.py, aunque esta función usa su propia sesión interna
    vía _fetch_custom_secuencial.
    Escribe JSON resultante a Bronze con entity='orders'.
    """
    log.info("[Orders] Extrayendo órdenes desde endpoint custom (fuente única)...")

    params_custom = {}
    if dtFrom:
        params_custom["searchCriteria[filterGroups][0][filters][0][field]"]          = "fecha_compra"
        params_custom["searchCriteria[filterGroups][0][filters][0][value]"]          = dtFrom
        params_custom["searchCriteria[filterGroups][0][filters][0][condition_type]"] = "gteq"
    if dtTo:
        params_custom["searchCriteria[filterGroups][1][filters][0][field]"]          = "fecha_compra"
        params_custom["searchCriteria[filterGroups][1][filters][0][value]"]          = dtTo
        params_custom["searchCriteria[filterGroups][1][filters][0][condition_type]"] = "lteq"

    items_custom = _fetch_custom_secuencial(params_custom, label="Orders custom")

    if not items_custom:
        return pd.DataFrame()

    orders = _construir_ordenes_desde_custom(items_custom)

    # Bronze: JSON construido íntegramente desde el custom
    write_raw_to_bronze(orders, "orders", dtFrom, dtTo)

    log.info(f"[Orders] {len(orders):,} órdenes ({len(items_custom):,} líneas) escritas a Bronze")
    return pd.DataFrame(orders)


# ---------------------------------------------------------------------------
# SECCIÓN: Orders actualizadas — endpoint nativo
# ---------------------------------------------------------------------------
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
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Orders actualizadas")
    write_raw_to_bronze(items, "orders_updated", dtFrom, dtTo)
    log.info(f"[Orders actualizadas] {len(items):,} órdenes extraídas.")
    return pd.DataFrame(items) if items else pd.DataFrame()


# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Aprobados
# ---------------------------------------------------------------------------
def extraer_clientes_aprobados(
    session: requests.Session,
    dtFrom:  str,
    dtTo:    str,
) -> pd.DataFrame:
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
    write_raw_to_bronze(items, "clients", dtFrom, dtTo)
    log.info(f"[Clientes aprobados] {len(items):,} extraídos.")
    return pd.DataFrame(items) if items else pd.DataFrame()


# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Pendientes
# ---------------------------------------------------------------------------
def extraer_clientes_pendientes(session: requests.Session) -> pd.DataFrame:
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
        resp  = session.get(url, timeout=120)
        resp.raise_for_status()
        data  = _parse_json_response(resp, label="Clientes pendientes")
        items = data if isinstance(data, list) else data.get("items", [])

        if not items:
            break

        all_items.extend(items)
        log.info(f"[Clientes pendientes] Página {page}: {len(items)} registros")

        if len(items) < PAGE_SIZE:
            break

        page += 1

    write_raw_to_bronze(all_items, "clients_pending")
    log.info(f"[Clientes pendientes] Total extraídos: {len(all_items):,}")
    return pd.DataFrame(all_items) if all_items else pd.DataFrame()


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

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    write_raw_to_bronze(items, "products", date_from=last_sync or now, date_to=now)

    log.info(f"[Productos] {len(items):,} extraídos.")
    return pd.DataFrame(items) if items else pd.DataFrame()

# ---------------------------------------------------------------------------
# SECCIÓN: Company — full reload por ID (sin delta por fecha disponible)
# ---------------------------------------------------------------------------
# El endpoint no expone updated_at de status — no se puede detectar cuándo
# una compañía existente cambió de estado (bloqueo/desbloqueo). Mientras TI
# no agregue ese campo, se hace checkpoint por ID: cada corrida solo trae
# compañías NUEVAS (id > último id ingestado). Un cambio de status en una
# compañía ya existente NO se refleja hasta que exista delta real por fecha
# (pendiente — solicitado a TI).
# Volumen total ~120K registros (validado 2026-08) — paginado 500 por página.
# Usa CUSTOM_TOKEN (no el token nativo de build_session) — confirmado por
# el equipo que el endpoint /rest/V1/company se autentica igual que el custom.
# ---------------------------------------------------------------------------
def _fetch_companies_secuencial(last_id: int = 0, page_size: int = 500) -> list:
    page      = 1
    all_items = []
    headers   = {"Authorization": f"Bearer {CUSTOM_TOKEN}"}
    session   = requests.Session()

    # NOTA (2026-08): sin sortOrders — un intento inicial con
    # sortOrders[field]=id devolvió 500 Internal Server Error. Es probable
    # que el campo interno filtrable/ordenable no se llame "id" (el JSON
    # expone "id" pero Magento suele usar "entity_id" internamente para
    # este tipo de colecciones). Paginación simple por ahora, sin orden
    # explícito — igual que la prueba manual que sí funcionó.
    params_base = {}
    if last_id > 0:
        params_base["searchCriteria[filterGroups][0][filters][0][field]"]          = "entity_id"
        params_base["searchCriteria[filterGroups][0][filters][0][value]"]          = last_id
        params_base["searchCriteria[filterGroups][0][filters][0][condition_type]"] = "gt"

    while True:
        params_page = {
            **params_base,
            "searchCriteria[pageSize]":    page_size,
            "searchCriteria[currentPage]": page,
        }
        resp  = session.get(COMPANY_URL, params=params_page, headers=headers, timeout=60)
        resp.raise_for_status()
        data  = _parse_json_response(resp, label="Company")
        items = data if isinstance(data, list) else data.get("items", [])

        if not items:
            break

        all_items.extend(items)
        log.info(f"[Company] Página {page}: {len(items)} filas | Acumulado: {len(all_items):,}")

        if len(items) < page_size:
            break

        page += 1

    log.info(f"[Company] Total extraídos: {len(all_items):,} filas")
    return all_items


def extraer_companias(last_id: int = 0) -> pd.DataFrame:
    """
    Extrae compañías nuevas (id > last_id) desde el endpoint nativo
    /rest/V1/company. Escribe JSON crudo a Bronze con entity='companies'.
    Retorna DataFrame con la columna 'id' para que el caller calcule el
    nuevo checkpoint (MAX(id)) tras escribir a Bronze.
    """
    items = _fetch_companies_secuencial(last_id=last_id)

    if not items:
        return pd.DataFrame()

    write_raw_to_bronze(items, "companies")

    log.info(f"[Company] {len(items):,} compañías nuevas escritas a Bronze")
    return pd.DataFrame(items)
