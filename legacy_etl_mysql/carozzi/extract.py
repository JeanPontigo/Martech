"""
extract.py
==========
Responsabilidad: obtener datos crudos desde la API de Magento (Carozzi).

Cambios respecto al original:
- HTTP Session compartida con connection pooling + retry/backoff (build_session)
- Paginación paralela con ThreadPoolExecutor vía fetch_all_pages()
- Delta loading por updated_at gestionado desde pipeline_state en DB
- Todas las funciones retornan DataFrames en memoria (sin escritura a Excel)
- Eliminadas: _get_with_retries manual, actualizar_excel, validacion_entrada
- Logging estructurado (sin print ni tqdm)

Nota: extraer_category usa recursión sobre el árbol de nodos — permanece
secuencial por naturaleza. No es compatible con fetch_all_pages().
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

import pandas as pd
import requests

from utils import build_url, build_session, MAX_WORKERS

log = logging.getLogger("pipeline.extract")

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PAGE_SIZE_PRODUCTS      = 250
PAGE_SIZE_ORDERS        = 100   # Reducido — servidor Carozzi colapsa con páginas grandes en paralelo
PAGE_SIZE_CUSTOMERS     = 400
PAGE_SIZE_ORDER_ITEMS   = 500
PAGE_SIZE_GROUPS        = 20
THROTTLE_SLEEP          = 0.2
MAX_WORKERS_ORDERS      = 2     # Solo 2 workers para orders — endpoint sensible a carga paralela


# ---------------------------------------------------------------------------
# Primitivos de paginación paralela
# ---------------------------------------------------------------------------
def _fetch_total_count(session: requests.Session, url_base: str) -> int:
    """Un solo request para obtener total_count."""
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
        log.info("[%s] Sin registros nuevos.", label)
        return []

    total_pages = -(-total // page_size)
    log.info("[%s] %s registros → %s páginas (pageSize=%s, workers=%s)",
             label, f"{total:,}", total_pages, page_size, max_workers)

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
                log.warning("[%s] Error en página %s: %s", label, page_num, e)

    log.info("[%s] Extraídos: %s", label, f"{len(all_items):,}")
    return all_items


# ---------------------------------------------------------------------------
# SECCIÓN: Categories
# ---------------------------------------------------------------------------
def extraer_category(session: requests.Session) -> pd.DataFrame:
    """
    Extrae el árbol de categorías recursivamente.
    La API devuelve un árbol anidado (children_data), no una lista paginada,
    por lo que no es compatible con fetch_all_pages() — permanece secuencial.
    """
    url = build_url("/rest/V1/categories")
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    tree = resp.json()
    root_id = tree.get("id")
    filas = []

    def _flatten(node, parent_path="", level=0):
        name = node.get("name") or ""
        path = f"{parent_path} / {name}".strip(" /") if (parent_path or name) else ""

        if level == 0:
            clasificacion = "root"
        elif node.get("parent_id") == root_id:
            clasificacion = "categoria"
        else:
            clasificacion = "subcategoria"

        filas.append({
            "category_id":   node.get("id"),
            "parent_id":     node.get("parent_id"),
            "category_name": name,
            "is_active":     node.get("is_active"),
            "level":         level,
            "path":          path,
            "tipo":          clasificacion,
            "product_count": node.get("product_count"),
        })
        for child in node.get("children_data") or []:
            _flatten(child, path, level + 1)

    _flatten(tree)
    df = pd.DataFrame(filas)
    log.info("[Category] %s categorías extraídas.", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def extraer_productos(session: requests.Session, last_sync: str = None) -> pd.DataFrame:
    """
    Extrae productos con delta loading por updated_at.
    Siempre extrae productos tipo bundle sin filtro de fecha — los bundles
    tienen SKUs con prefijo especial y su product_id aparece en orders_items,
    por lo que deben estar siempre presentes en la tabla product.
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

    # Siempre extraer bundles completos — no aplica delta loading
    # porque necesitamos su entity_id para el join con orders_items
    url_bundles = build_url(
        "/rest/V1/products"
        "?searchCriteria[sortOrders][0][field]=updated_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
        "&searchCriteria[filterGroups][0][filters][0][field]=type_id"
        "&searchCriteria[filterGroups][0][filters][0][value]=bundle"
        "&searchCriteria[filterGroups][0][filters][0][condition_type]=eq"
    )
    bundles = fetch_all_pages(session, url_bundles, PAGE_SIZE_PRODUCTS, label="Productos bundle")

    # Mergear evitando duplicados por id
    all_items = items + bundles
    seen = set()
    unique_items = []
    for item in all_items:
        item_id = item.get("id")
        if item_id not in seen:
            seen.add(item_id)
            unique_items.append(item)

    if not unique_items:
        return pd.DataFrame()

    rows = []
    for item in unique_items:
        ext       = item.get("extension_attributes") or {}
        cat_links = sorted(
            [c for c in (ext.get("category_links") or []) if isinstance(c, dict)],
            key=lambda c: c.get("position", 0),
        )
        prod_links = [l for l in (item.get("product_links") or []) if isinstance(l, dict)]
        marcas     = item.get("custom_attributes") or []

        category_ids  = ", ".join(str(c.get("category_id")) for c in cat_links if c.get("category_id") is not None)
        positions     = ", ".join(str(c.get("position"))    for c in cat_links if c.get("position")    is not None)
        linked_types  = ", ".join(str(l.get("linked_product_type")) for l in prod_links if l.get("linked_product_type"))
        marca_val     = ", ".join(str(g.get("value")) for g in marcas if isinstance(g, dict) and (g.get("attribute_code") or "").casefold() == "marca")
        imagen_val    = ", ".join(str(g.get("value")) for g in marcas if isinstance(g, dict) and (g.get("attribute_code") or "").casefold() == "image")

        rows.append({
            "entity_id":           item.get("id"),
            "sku":                 item.get("sku"),
            "sku_name":            item.get("name"),
            "price":               item.get("price"),
            "status":              item.get("status"),
            "visibility":          item.get("visibility"),
            "type_id":             item.get("type_id"),
            "created_at":          item.get("created_at"),
            "updated_at":          item.get("updated_at"),
            "linked_product_type": linked_types or None,
            "product_image":       imagen_val or None,
            "peso_promedio_kg":    item.get("weight"),
            "marca":               marca_val or None,
            "category_id":         category_ids or None,
            "category_name":       None,
            "sub_category":        None,
            "position":            positions or None,
        })

    df = pd.DataFrame(rows)
    log.info("[Productos] %s productos extraídos.", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Customers
# ---------------------------------------------------------------------------
def extraer_customers(session: requests.Session, last_sync: str = None) -> pd.DataFrame:
    """
    Extrae clientes. Con last_sync filtra por updated_at (delta loading).
    """
    url_base = build_url(
        "/rest/V1/customers/search"
        "?searchCriteria[sortOrders][0][field]=updated_at"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    if last_sync:
        url_base += (
            "&searchCriteria[filterGroups][0][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][0][filters][0][value]={last_sync}"
            "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        )

    items = fetch_all_pages(session, url_base, PAGE_SIZE_CUSTOMERS, label="Customers")
    if not items:
        return pd.DataFrame()

    rows = []
    for item in items:
        addresses         = item.get("addresses") or []
        custom_attributes = {
            a.get("attribute_code"): a.get("value")
            for a in (item.get("custom_attributes") or [])
            if isinstance(a, dict)
        }
        rut = custom_attributes.get("rut") or custom_attributes.get("rut_register")

        company = city = region = None
        if addresses:
            company = addresses[0].get("company")
            city    = addresses[0].get("city")
            region  = (addresses[0].get("region") or {}).get("region")

        rows.append({
            "customer_id":         item.get("id"),
            "customer_group_id":   item.get("group_id"),
            "customer_group_name": item.get("group_id"),
            "created_at":          item.get("created_at"),
            "updated_at":          item.get("updated_at"),
            "customer_email":      item.get("email"),
            "customer_firstname":  item.get("firstname"),
            "customer_lastname":   item.get("lastname"),
            "company":             company,
            "rut":                 rut,
            "region":              region,
            "city":                city,
        })

    df = pd.DataFrame(rows)
    log.info("[Customers] %s clientes extraídos.", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Orders
# ---------------------------------------------------------------------------
def _aplanar_orders(items: list) -> pd.DataFrame:
    """Aplana campos de la respuesta de /rest/V1/orders."""
    if not items:
        return pd.DataFrame()
    rows = []
    for item in items:
        ba = item.get("billing_address") or {}
        pa = item.get("payment") or {}
        rows.append({
            "order_id":          item.get("entity_id"),
            "base_grand_total":  item.get("base_grand_total"),
            "created_at":        item.get("created_at"),
            "updated_at":        item.get("updated_at"),
            "grand_total":       item.get("grand_total"),
            "shipping_amount":   item.get("shipping_amount"),
            "customer_id":       item.get("customer_id"),
            "customer_email":    item.get("customer_email"),
            "state":             item.get("state"),
            "status":            item.get("status"),
            "subtotal":          item.get("subtotal"),
            "discount_amount":   item.get("discount_amount"),
            "order_city":        ba.get("city"),
            "order_region":      ba.get("region"),
            "peso_promedio_kg":  item.get("weight"),
            "method":            pa.get("method"),
            "total_qty_ordered": item.get("total_qty_ordered"),
            "total_item_count":  item.get("total_item_count"),
        })
    return pd.DataFrame(rows)


def _build_orders_url(fields: str) -> str:
    """URL base del endpoint de órdenes con fields filter."""
    return build_url(
        "/rest/V1/orders"
        f"?fields=items[{fields}],total_count"
    )


_ORDERS_FIELDS = (
    "entity_id,base_grand_total,created_at,updated_at,grand_total,"
    "shipping_amount,customer_email,customer_id,state,status,subtotal,discount_amount,"
    "weight,total_qty_ordered,total_item_count,"
    "billing_address[city,region],payment[method]"
)


def extraer_orders_nuevas(session: requests.Session, dt_from: str, dt_to: str) -> pd.DataFrame:
    """
    Extrae órdenes NUEVAS filtrando por created_at.
    Se usa para INSERT — solo trae órdenes creadas en el rango.
    """
    url_base = (
        _build_orders_url(_ORDERS_FIELDS)
        + "&searchCriteria[sortOrders][0][field]=created_at"
        + "&searchCriteria[sortOrders][0][direction]=ASC"
        + "&searchCriteria[filterGroups][0][filters][0][field]=created_at"
        + f"&searchCriteria[filterGroups][0][filters][0][value]={dt_from}"
        + "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        + "&searchCriteria[filterGroups][1][filters][0][field]=created_at"
        + f"&searchCriteria[filterGroups][1][filters][0][value]={dt_to}"
        + "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Orders nuevas", max_workers=MAX_WORKERS_ORDERS)
    df = _aplanar_orders(items)
    log.info("[Orders nuevas] %s órdenes extraídas.", f"{len(df):,}")
    return df


def extraer_orders_actualizadas(session: requests.Session, dt_from: str, dt_to: str) -> pd.DataFrame:
    """
    Extrae órdenes MODIFICADAS filtrando por updated_at.
    Se usa para UPDATE de status — trae órdenes que cambiaron en el rango,
    incluyendo órdenes viejas que cambiaron de status recientemente.
    """
    url_base = (
        _build_orders_url(_ORDERS_FIELDS)
        + "&searchCriteria[sortOrders][0][field]=updated_at"
        + "&searchCriteria[sortOrders][0][direction]=ASC"
        + "&searchCriteria[filterGroups][0][filters][0][field]=updated_at"
        + f"&searchCriteria[filterGroups][0][filters][0][value]={dt_from}"
        + "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        + "&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
        + f"&searchCriteria[filterGroups][1][filters][0][value]={dt_to}"
        + "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Orders actualizadas", max_workers=MAX_WORKERS_ORDERS)
    df = _aplanar_orders(items)
    log.info("[Orders actualizadas] %s órdenes extraídas.", f"{len(df):,}")
    return df


def extraer_orders(session: requests.Session, last_sync: str = None) -> pd.DataFrame:
    """
    Compatibilidad con order_items — extrae órdenes por updated_at
    para el delta loading de items.
    """
    url_base = (
        _build_orders_url(_ORDERS_FIELDS)
        + "&searchCriteria[sortOrders][0][field]=updated_at"
        + "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    if last_sync:
        url_base += (
            "&searchCriteria[filterGroups][0][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][0][filters][0][value]={last_sync}"
            "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDERS, label="Orders", max_workers=MAX_WORKERS_ORDERS)
    df = _aplanar_orders(items)
    log.info("[Orders] %s órdenes extraídas.", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Order Items
# ---------------------------------------------------------------------------
_ORDER_ITEMS_FIELDS = (
    "item_id,order_id,created_at,updated_at,product_id,"
    "product_type,original_price,qty_ordered,qty_invoiced,qty_shipped,sku"
)

def _aplanar_order_items(items: list) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()
    rows = []
    for item in items:
        rows.append({
            "item_id":        item.get("item_id"),
            "order_id":       item.get("order_id"),
            "created_at":     item.get("created_at"),
            "updated_at":     item.get("updated_at"),
            "product_id":     item.get("product_id"),
            "product_type":   item.get("product_type"),
            "original_price": item.get("original_price"),
            "qty_ordered":    item.get("qty_ordered"),
            "qty_invoiced":   item.get("qty_invoiced"),
            "qty_shipped":    item.get("qty_shipped"),
            "sku":            item.get("sku"),
        })
    return pd.DataFrame(rows)


def extraer_orders_items_nuevos(session: requests.Session, dt_from: str, dt_to: str) -> pd.DataFrame:
    """
    Extrae items NUEVOS filtrando por created_at.
    Captura items de órdenes nuevas o SKUs agregados a órdenes existentes.
    """
    url_base = (
        build_url("/rest/V1/orders/items")
        + f"?fields=items[{_ORDER_ITEMS_FIELDS}],total_count"
        + "&searchCriteria[sortOrders][0][field]=created_at"
        + "&searchCriteria[sortOrders][0][direction]=ASC"
        + "&searchCriteria[filterGroups][0][filters][0][field]=created_at"
        + f"&searchCriteria[filterGroups][0][filters][0][value]={dt_from}"
        + "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        + "&searchCriteria[filterGroups][1][filters][0][field]=created_at"
        + f"&searchCriteria[filterGroups][1][filters][0][value]={dt_to}"
        + "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDER_ITEMS, label="Order items nuevos", max_workers=MAX_WORKERS_ORDERS)
    df = _aplanar_order_items(items)
    log.info("[Order items nuevos] %s items extraídos.", f"{len(df):,}")
    return df


def extraer_orders_items_actualizados(session: requests.Session, dt_from: str, dt_to: str) -> pd.DataFrame:
    """
    Extrae items MODIFICADOS filtrando por updated_at.
    Captura cancelaciones, cambios de cantidad, cambios de estado.
    """
    url_base = (
        build_url("/rest/V1/orders/items")
        + f"?fields=items[{_ORDER_ITEMS_FIELDS}],total_count"
        + "&searchCriteria[sortOrders][0][field]=updated_at"
        + "&searchCriteria[sortOrders][0][direction]=ASC"
        + "&searchCriteria[filterGroups][0][filters][0][field]=updated_at"
        + f"&searchCriteria[filterGroups][0][filters][0][value]={dt_from}"
        + "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        + "&searchCriteria[filterGroups][1][filters][0][field]=updated_at"
        + f"&searchCriteria[filterGroups][1][filters][0][value]={dt_to}"
        + "&searchCriteria[filterGroups][1][filters][0][condition_type]=lteq"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDER_ITEMS, label="Order items actualizados", max_workers=MAX_WORKERS_ORDERS)
    df = _aplanar_order_items(items)
    log.info("[Order items actualizados] %s items extraídos.", f"{len(df):,}")
    return df


def extraer_orders_items(session: requests.Session, last_sync: str = None) -> pd.DataFrame:
    """
    Compatibilidad con full load — extrae todos los items por updated_at.
    """
    url_base = (
        build_url("/rest/V1/orders/items")
        + f"?fields=items[{_ORDER_ITEMS_FIELDS}],total_count"
        + "&searchCriteria[sortOrders][0][field]=updated_at"
        + "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    if last_sync:
        url_base += (
            "&searchCriteria[filterGroups][0][filters][0][field]=updated_at"
            f"&searchCriteria[filterGroups][0][filters][0][value]={last_sync}"
            "&searchCriteria[filterGroups][0][filters][0][condition_type]=gteq"
        )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_ORDER_ITEMS, label="Order items", max_workers=MAX_WORKERS_ORDERS)
    df = _aplanar_order_items(items)
    log.info("[Order items] %s items extraídos.", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Marcas
# ---------------------------------------------------------------------------
def extraer_marcas(session: requests.Session) -> pd.DataFrame:
    """
    Extrae el catálogo de marcas desde el atributo 'marca'.
    Es un endpoint de atributos — sin paginación, sin delta loading.
    """
    url = build_url("/rest/default/V1/products/attributes/marca")
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    items = resp.json().get("options", [])

    rows = [{"label": i.get("label"), "value": i.get("value")} for i in items]
    df = pd.DataFrame(rows)
    log.info("[Marcas] %s marcas extraídas.", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Customer Groups
# ---------------------------------------------------------------------------
def extraer_customers_group(session: requests.Session) -> pd.DataFrame:
    """
    Extrae grupos de clientes. Volumen pequeño y estático — paginación
    paralela disponible pero innecesaria en la práctica.
    """
    url_base = build_url(
        "/rest/V1/customerGroups/search"
        "?searchCriteria[sortOrders][0][field]=id"
        "&searchCriteria[sortOrders][0][direction]=ASC"
    )
    items = fetch_all_pages(session, url_base, PAGE_SIZE_GROUPS, label="Customer groups")
    if not items:
        return pd.DataFrame()

    rows = [{
        "customer_group_id": i.get("id"),
        "code":              i.get("code"),
        "tax_class_id":      i.get("tax_class_id"),
        "tax_class_name":    i.get("tax_class_name"),
    } for i in items]

    df = pd.DataFrame(rows)
    log.info("[Customer groups] %s grupos extraídos.", f"{len(df):,}")
    return df
