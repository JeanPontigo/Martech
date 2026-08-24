"""
transform.py  — Pipeline PF B2B
=================================
Responsabilidad: limpiar, normalizar y estructurar los DataFrames
extraídos antes de cargarlos en la base de datos.

Estándar aplicado (patrón Ariztia):
- print() reemplazados por logging estructurado
- Imports al tope del módulo (no inline)
- Mappings (marcas, grupos, categorías) recibidos como parámetros
  en lugar de llamar la API dentro del transform (sin side effects)
- Lógica de negocio preservada intacta
"""

import ast
import logging

import numpy as np
import pandas as pd

log = logging.getLogger("pipeline.transform")

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------
def _normalize_rut(series: pd.Series) -> pd.Series:
    """Convierte un RUT a formato #####-x (sin puntos, con guión, k minúscula)."""
    def _norm(value):
        if not isinstance(value, str):
            return value
        v = value.replace(".", "").lower().strip()
        if not v:
            return v
        if "-" not in v and len(v) > 1:
            v = f"{v[:-1]}-{v[-1]}"
        return v
    return series.apply(_norm)


def _parse_id_list(val) -> list:
    """
    Devuelve siempre una lista de IDs (puede ser vacía).
    Acepta: "[12,34]", "278", [12, 34], np.nan, None.
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []
    if isinstance(val, (list, tuple)):
        return list(val)
    if isinstance(val, str):
        v = val.strip()
        if v in ("", "[]"):
            return []
        if v.startswith("["):
            try:
                return list(ast.literal_eval(v))
            except Exception:
                return []
        return [v]
    return [val]


# ---------------------------------------------------------------------------
# SECCIÓN: Órdenes
# ---------------------------------------------------------------------------
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el DataFrame de cabeceras de órdenes.
    Mapeo de campos API → DB:
        order_id       → order_id
        fecha          → created_at
        updated_at     → updated_at
        estado         → estado
        total          → total
        subtotal       → subtotal
        descuento      → descuento
        envio          → envio
        client_id      → client_id
        company_id     → company_id
        payment_method → payment_method
        ciudad         → ciudad
        region         → region
        fecha_envio    → fecha_envio
    """
    if df.empty:
        log.info("[transform] transform_orders → DataFrame vacío")
        return df

    df = df.copy()

    # Deduplicar a nivel de orden (el df viene expandido por items)
    orden_cols = [
        "order_id", "fecha", "updated_at", "fecha_envio", "ciudad_envio",
        "estado", "total", "subtotal", "descuento", "envio",
        "client_id", "cliente_email", "group_id",
        "ciudad", "region", "payment_method", "coupon_code",
        "company_id", "company_name",
    ]
    orden_cols_presentes = [c for c in orden_cols if c in df.columns]
    df_orders = df[orden_cols_presentes].drop_duplicates(subset="order_id", keep="first").copy()

    # Fechas
    df_orders["created_at"]  = pd.to_datetime(df_orders["fecha"], errors="coerce")
    df_orders["updated_at"]  = pd.to_datetime(df_orders["updated_at"], errors="coerce")
    df_orders["fecha_envio"] = pd.to_datetime(df_orders.get("fecha_envio"), errors="coerce")

    # Numéricos
    for col in ["total", "subtotal", "descuento", "envio"]:
        if col in df_orders.columns:
            df_orders[col] = pd.to_numeric(df_orders[col], errors="coerce").fillna(0)

    # Texto
    df_orders["estado"]         = df_orders["estado"].astype(str).str.lower().str.strip()
    df_orders["payment_method"] = df_orders["payment_method"].astype(str).str.lower().str.strip()
    df_orders["cliente_email"]  = df_orders["cliente_email"].astype(str).str.lower().str.strip()
    df_orders["company_name"]   = df_orders.get("company_name", "").fillna("Sin empresa").astype(str).str.title()

    for col in ["ciudad", "ciudad_envio", "region"]:
        if col in df_orders.columns:
            df_orders[col] = df_orders[col].astype(str).str.title().str.strip()

    # RUTs
    for col in df_orders.columns:
        if "rut" in col.lower():
            df_orders[col] = _normalize_rut(df_orders[col])

    # Columnas finales que espera la tabla orders
    col_order = [
        "order_id", "client_id", "company_id", "created_at", "updated_at",
        "estado", "total", "subtotal", "payment_method",
        "descuento", "envio", "fecha_envio", "ciudad", "region",
    ]
    col_order = [c for c in col_order if c in df_orders.columns]
    df_orders = df_orders[col_order].where(pd.notnull(df_orders[col_order]), None)

    log.info(f"[transform] transform_orders → {len(df_orders):,} órdenes")
    return df_orders


# ---------------------------------------------------------------------------
# SECCIÓN: Order Items
# ---------------------------------------------------------------------------
def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el DataFrame de líneas de orden.
    Mapeo:
        sku            → sku
        sku_name       → sku_name
        precio_unitario→ sku_value
        cantidad       → sku_qty
        total_linea    → sku_total_value
        base_price     → base_price
        original_price → original_price
        en_oferta      → en_oferta (derivado: base_price < original_price)
    """
    if df.empty:
        log.info("[transform] transform_order_items → DataFrame vacío")
        return df

    df = df.copy()

    df["sku"]      = df["sku"].astype(str).str.upper().str.strip()
    df["sku_name"] = df["sku_name"].astype(str).str.strip()

    for col in ["precio_unitario", "cantidad", "total_linea", "base_price", "original_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["en_oferta"] = df["base_price"] < df["original_price"]

    df = df.rename(columns={
        "precio_unitario": "sku_value",
        "cantidad":        "sku_qty",
        "total_linea":     "sku_total_value",
    })

    col_order = [
        "order_id", "sku", "sku_name",
        "sku_qty", "sku_value", "sku_total_value",
        "base_price", "original_price", "en_oferta",
    ]
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order].drop_duplicates(subset=["order_id", "sku"]).where(pd.notnull(df[col_order]), None)

    log.info(f"[transform] transform_order_items → {len(df):,} líneas")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def transform_productos(
    df: pd.DataFrame,
    brand_map: dict,
    category_map: dict,
) -> pd.DataFrame:
    """
    Normaliza productos.
    Recibe brand_map y category_map como parámetros
    (extraídos una sola vez en main.py, sin side effects aquí).
    """
    if df.empty:
        log.info("[transform] transform_productos → DataFrame vacío")
        return df

    df = df.copy()

    # Deduplicar por PK
    before = len(df)
    df = df.drop_duplicates("sku", keep="first")
    if before != len(df):
        log.info(f"[transform] product: {before - len(df)} duplicados descartados (sku)")

    # Texto y tipos
    df["sku"]      = df["sku"].astype(str).str.upper().str.strip()
    df["sku_name"] = df["sku_name"].astype(str).str.title().str.strip()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["peso_promedio_kg"]  = pd.to_numeric(df.get("peso_promedio_kg"), errors="coerce")
    df["unidad_x_producto"] = pd.to_numeric(df.get("unidad_x_producto"), errors="coerce")

    # Marca
    df["marca_logo"] = df["marca_logo"].astype(str).str.strip()
    df["marca"] = df["marca_logo"].apply(
        lambda x: brand_map.get(x, "Sin Marca")
    ).str.title()

    # Categoría / Subcategoría desde category_id (puede ser lista "[12, 34]")
    def _cat_names(val):
        ids = _parse_id_list(val)
        cat = category_map.get(str(ids[0]),  "Sin Categoría")    if ids else "Sin Categoría"
        sub = category_map.get(str(ids[1]), "Sin Subcategoría") if len(ids) > 1 else "Sin Subcategoría"
        return pd.Series([cat, sub])

    df[["category", "sub_category"]] = df["category_id"].apply(_cat_names)

    # Extraer el último ID de la lista como FK a category
    def _last_cat_id(val):
        ids = _parse_id_list(val)
        if not ids:
            return pd.NA
        try:
            return int(ids[-1])
        except (ValueError, TypeError):
            return pd.NA

    df["category_id"] = df["category_id"].apply(_last_cat_id).astype("Int64")

    # Imagen → URL completa
    base_img = "https://tiendapfalimentos.cl/media/catalog/product/"
    df["image"] = df["image"].fillna("").astype(str).str.strip()
    mask = df["image"] != ""
    df.loc[mask, "image"] = base_img + df.loc[mask, "image"].str.lstrip("/")

    df = df.rename(columns={"image": "image_sku"})

    col_order = [
        "sku", "sku_name", "product_type", "availability",
        "created_at", "updated_at",
        "peso_promedio_kg", "unidad_x_producto",
        "marca", "category_id", "category", "sub_category", "image_sku",
    ]
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order].where(pd.notnull(df[col_order]), None)

    log.info(f"[transform] transform_productos → {len(df):,} productos")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Clientes
# ---------------------------------------------------------------------------
def transform_clientes(
    df: pd.DataFrame,
    group_map: dict,
) -> pd.DataFrame:
    """
    Normaliza clientes.
    Recibe group_map como parámetro (sin side effects).
    """
    if df.empty:
        log.info("[transform] transform_clientes → DataFrame vacío")
        return pd.DataFrame(columns=[
            "client_id", "group_id", "group_name", "created_at", "updated_at",
            "client_email", "firstname", "lastname", "client_rut",
            "company_id", "company_rut",
        ])

    df = df.copy()

    # Rename PK y campos
    df = df.rename(columns={
        "id":    "client_id",
        "email": "client_email",
        "rut":   "client_rut",
    })

    # Deduplicar por PK
    before = len(df)
    df = df.drop_duplicates("client_id", keep="first")
    if before != len(df):
        log.info(f"[transform] client: {before - len(df)} duplicados descartados (client_id)")

    # Grupo de cliente
    df["group_name"] = (
        df["group_id"].map(group_map).fillna("Sin Grupo").astype(str).str.upper()
    )

    # Fechas
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    # Texto
    df["client_email"] = df["client_email"].astype(str).str.lower().str.strip()
    df["firstname"]    = df["firstname"].astype(str).str.title().str.strip()
    df["lastname"]     = df["lastname"].astype(str).str.title().str.strip()
    df["client_rut"]   = _normalize_rut(df["client_rut"].astype(str))

    # company_id y company_rut
    if "company_id" not in df.columns:
        df["company_id"] = pd.NA
    if "company_rut" not in df.columns:
        df["company_rut"] = pd.NA
    else:
        df["company_rut"] = df["company_rut"].astype(str).str.strip()

    col_order = [
        "client_id", "group_id", "group_name",
        "created_at", "updated_at",
        "client_email", "firstname", "lastname", "client_rut",
        "company_id", "company_rut",
    ]
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order].where(pd.notnull(df[col_order]), None)

    log.info(f"[transform] transform_clientes → {len(df):,} clientes")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Compañías
# ---------------------------------------------------------------------------
def transform_companias(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.info("[transform] transform_companias → DataFrame vacío")
        return df

    df = df.copy()
    df["company_name"] = df["company_name"].astype(str).str.title().str.strip()
    df["rut_company"]  = _normalize_rut(df["rut_company"].astype(str))
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] transform_companias → {len(df):,} compañías")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Categorías
# ---------------------------------------------------------------------------
def transform_categorias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida integridad del árbol: parent_id debe existir en category_id
    o ser NULL (raíz). Categorías huérfanas se fijan a NULL.
    """
    if df.empty:
        log.info("[transform] transform_categorias → DataFrame vacío")
        return df

    df = df.copy()
    cats = set(df["category_id"].astype(int))
    df.loc[~df["parent_id"].isin(cats), "parent_id"] = pd.NA
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] transform_categorias → {len(df):,} categorías")
    return df
