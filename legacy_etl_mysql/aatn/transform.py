"""
transform.py
============
Responsabilidad: limpiar, normalizar y estructurar los DataFrames
extraídos antes de cargarlos en la base de datos.

Cambios respecto al original:
- transform_orders  → mapea campos del endpoint custom
- transform_grid    → mapea campos del endpoint custom (filas directas)
- process_order_data → adaptado a campos del custom
- Eliminado: enriquecer_ordenes (ya no necesario)
- Sin cambios: transform_clientes, transform_productos, transform_clientes_pendientes
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger("pipeline.transform")

# ---------------------------------------------------------------------------
# SECCIÓN: Order Info
# ---------------------------------------------------------------------------
def process_order_data(df_orders_info: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae info_orders desde el DataFrame del custom.
    El custom no trae payment ni customer_group_id — se registran como None.
    order_id viene como increment_id normalizado (sin ceros).
    """
    if df_orders_info.empty:
        return pd.DataFrame()

    df = df_orders_info.copy()

    # order_id ya viene normalizado desde transform_orders
    if "order_id" not in df.columns and "entity_id" in df.columns:
        df = df.rename(columns={"entity_id": "order_id"})

    for col in ["order_id", "payment_method", "customer_group"]:
        if col not in df.columns:
            df[col] = None

    df = df[["order_id", "payment_method", "customer_group"]].copy()

    # El custom no trae método de pago ni grupo — se llenan como None
    df["payment_method"]  = None
    df["customer_group"]  = None

    conversion_grupos = {
        1:  "Cliente General",
        5:  "Cliente Tradicional",
        8:  "Cliente Food Service",
        11: "Cliente Mayorista",
        14: "Cliente SPM Regional",
        20: "Cliente Industrial",
        23: "Cliente Particulares",
        26: "Cliente Otros",
    }
    df["client_group"] = df["customer_group"].map(conversion_grupos).fillna("Nuevo Cliente")
    df["order_id"]     = pd.to_numeric(df["order_id"], errors="coerce").fillna(0).astype(int)
    df = df[["order_id", "payment_method", "customer_group", "client_group"]]
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] process_order_data → {len(df):,} registros")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Aprobados
# ---------------------------------------------------------------------------
def transform_clientes(df: pd.DataFrame) -> pd.DataFrame:
    column_rename_map = {
        "entity_id":    "entity_id",
        "adobe_id":     "id_adobe",
        "id_sap":       "id_sap",
        "usu_rut":      "usu_rut",
        "centro":       "centro",
        "status":       "active",
        "razon_social": "razon_social",
        "contacto":     "contacto",
        "celular":      "celular",
        "email":        "email",
        "created_at":   "created_at",
        "last_login":   "last_conection",
        "region":       "region",
    }
    df = df.rename(columns=column_rename_map)
    if df.empty:
        log.info("[transform] transform_clientes → DataFrame vacío")
        return df
     
    columnas_a_string = ["celular", "email", "region", "usu_rut", "razon_social", "contacto"]
    for col in columnas_a_string:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").str.strip()

    df["id_sap"]     = pd.to_numeric(df["id_sap"], errors="coerce", downcast="integer")
    df["id_sap"]     = df["id_sap"].where(pd.notnull(df["id_sap"]), None)
    df["centro"]     = df["centro"].where(pd.notnull(df["centro"]), None)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    if "celular" in df.columns:
        df["celular"]    = df["celular"].str.replace(r"\D", "", regex=True)
    if "email" in df.columns:    
        df["email"]      = df["email"].str.lower().str.strip()
    if "region" in df.columns:
        df["region"]     = df["region"].str.lower().str.strip()
    if "razon_social" in df.columns:
        df["razon_social"] = df["razon_social"].str.lower().str.strip()
    if "contacto" in df.columns:
        df["contacto"]   = df["contacto"].str.lower().str.strip()
    if "usu_rut" in df.columns:
        df["usu_rut"]    = df["usu_rut"].str.replace(".", "", regex=False)

    for col in ["razon_social", "contacto", "region"]:
        if col in df.columns:
            df[col] = df[col].str.replace(",", " -").str.strip()

    if "razon_social" in df.columns:
        df["razon_social"] = df["razon_social"].str.replace(r'"', "", regex=True).str.strip()
    if "contacto" in df.columns:
        df["contacto"]     = df["contacto"].str.replace(r'"', "", regex=True).str.strip()
    df["last_updated"] = None

    df = df.drop_duplicates(subset=["email", "id_sap"])

    column_order = [
        "entity_id", "id_adobe", "id_sap", "usu_rut", "centro", "region", "active",
        "razon_social", "contacto", "celular", "email", "created_at",
        "last_conection", "last_updated",
    ]
    df = df.reindex(columns=column_order)
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] transform_clientes → {len(df):,} registros")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
_CUSTOM_ATTR_MAP = {
    "peso_por_caja":              "weight_per_box",
    "image":                      "image_url",
    "unidad_de_medida_contenido": "content_measurement_unit",
    "descripcion_categoria":      "category",
    "descripcion_subcategoria":   "subcategory",
    "marca":                      "brand",
    "unidades_por_caja":          "units_per_box",
    "peso_promedio_por_caja":     "average_weight_per_box",
    "dimensiones_de_caja":        "box_size",
    "descripcion_conservacion":   "storage_description",
}

def _parse_product(item: dict) -> dict:
    """Expande custom_attributes en columnas planas."""
    attrs = {a["attribute_code"]: a["value"] for a in item.get("custom_attributes", [])}
    return {
        "id":         item.get("id"),
        "sku":        item.get("sku"),
        "name":       item.get("name"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        **{v: attrs.get(k) for k, v in _CUSTOM_ATTR_MAP.items()},
    }

def transform_productos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "custom_attributes" in df.columns:
        df = pd.DataFrame([_parse_product(row) for row in df.to_dict("records")])

    df = df[df["sku"].astype(str).str.isnumeric()]
    df["sku"] = df["sku"].astype(int)

    base_url = "https://www.ariztiaatunegocio.cl/media/catalog/product"
    df["image_url"] = df["image_url"].apply(
        lambda x: f"{base_url}{x}" if pd.notnull(x) and str(x).startswith("/") else x
    )
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] transform_productos → {len(df):,} registros")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Grid — desde endpoint custom
# ---------------------------------------------------------------------------
def transform_grid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el grid extraído desde el endpoint custom.
    Cada fila ya es un item de orden — no hay que expandir items[].

    Mapeo de campos custom → DB:
        order_id (lstrip "0") → order_id  (int, entity_id en ariztia_orders)
        sap_id                → order_sap
        fecha_compra          → fecha_compra + fecha_unificada
        hora_compra           → hora_compra
        sku                   → sku
        name                  → name
        product_entity_id     → product_id
        item_id               → item_id
        marca                 → marca
        price                 → precio
        cantidad              → cantidad
        venta_neta            → venta_neta
        venta_bruta           → venta_bruta
        status                → status
    """
    if df.empty:
        log.info("[transform] transform_grid → DataFrame vacío")
        return df

    df = df.copy()

    # order_id del custom es increment_id con ceros ("000780522") → normalizar a int
    df["order_id"] = (
        df["order_id"].astype(str).str.lstrip("0").str.strip()
        .replace("", "0")
    )
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").fillna(0).astype(int)

    # order_sap
    df["order_sap"] = df["sap_id"].fillna("").astype(str).str.strip()

    # Fecha unificada desde fecha_compra + hora_compra del custom (ya en UTC-4)
    fecha_hora = df["fecha_compra"].astype(str) + " " + df["hora_compra"].astype(str)
    df["fecha_unificada"] = pd.to_datetime(fecha_hora, errors="coerce")
    df["fecha_compra"]    = df["fecha_unificada"].dt.date.astype(str)
    df["hora_compra"]     = df["fecha_unificada"].dt.time.astype(str)

    # SKU como string
    df["sku"]   = df["sku"].fillna("").astype(str).str.strip()
    df["marca"] = df["marca"].fillna("").astype(str).str.upper()

    # Conversiones numéricas
    for col in ["precio", "cantidad", "venta_neta", "venta_bruta"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Renombrar campos del custom al schema de ariztia_grid
    rename_map = {
        "price":             "precio",
        "product_entity_id": "product_id",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in ["product_id", "item_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Eliminar duplicados por orden + sku
    df = df.drop_duplicates(subset=["order_id", "sku"])

    column_order = [
        "order_id", "order_sap", "fecha_unificada", "fecha_compra", "hora_compra",
        "sku", "name", "product_id", "item_id", "marca",
        "precio", "cantidad", "venta_neta", "venta_bruta", "status",
    ]
    column_order = [c for c in column_order if c in df.columns]
    df = df[column_order]
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] transform_grid → {len(df):,} registros")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Orders — desde endpoint custom
# ---------------------------------------------------------------------------
def transform_orders(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma órdenes desde el endpoint custom.
    El custom entrega una fila por item — hay que agrupar por order_id
    para obtener una fila por orden.

    Mapeo de campos custom → DB:
        order_id (lstrip "0")  → order_id       (int, increment_id normalizado)
        sap_id                 → order_sap
        customer_sap_id        → id_sap_client  (local principal, lstrip "0")
        company_id             → id_destinatario
        rut_company            → rut
        centro                 → centro
        fecha_compra+hora      → fecha_unificada, fecha_compra, hora_compra
        fecha_despacho         → fecha_despacho
        direccion              → direccion
        comuna                 → comuna
        venta_neta (suma)      → venta_neta
        venta_bruta (suma)     → venta_bruta
        valor_envio            → valor_envio
        total_final            → total_final
        nombre_cupon           → nombre_cupon
        status                 → status
    """
    if df_raw.empty:
        log.info("[transform] transform_orders → DataFrame vacío")
        return df_raw

    df = df_raw.copy()

    # --- order_id: increment_id con ceros → int ---
    df["order_id"] = (
        df["order_id"].astype(str).str.lstrip("0").str.strip()
        .replace("", "0")
    )
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").fillna(0).astype(int)

    # --- order_sap ---
    df["order_sap"] = df["sap_id"].fillna("").astype(str).str.strip()

    # --- id_sap_client: customer_sap_id sin ceros → int ---
    df["id_sap_client"] = (
        df["customer_sap_id"].astype(str).str.lstrip("0").str.strip()
        .replace("", "0")
    )
    df["id_sap_client"] = pd.to_numeric(df["id_sap_client"], errors="coerce").fillna(0).astype(int)

    # --- id_destinatario: company_id → int ---
    df["id_destinatario"] = pd.to_numeric(df["company_id"], errors="coerce").fillna(0).astype(int)

    # --- Fechas: ya en UTC-4, construir fecha_unificada ---
    fecha_hora = df["fecha_compra"].astype(str) + " " + df["hora_compra"].astype(str)
    df["fecha_unificada"] = pd.to_datetime(fecha_hora, errors="coerce")
    df["fecha_compra"]    = df["fecha_unificada"].dt.date.astype(str)
    df["hora_compra"]     = df["fecha_unificada"].dt.time.astype(str)
    df["fecha_despacho"]  = pd.to_datetime(df.get("fecha_despacho"), errors="coerce").dt.date.astype(str)
    df["fecha_despacho"]  = df["fecha_despacho"].replace("NaT", None)

    # --- Dirección y comuna ---
    df["direccion"] = df["direccion"].fillna("").astype(str).str.lower().str.replace(",", " -").str.strip()
    df["comuna"]    = df["comuna"].fillna("").astype(str).str.lower().str.replace(",", " -").str.strip()

    # --- RUT ---
    df["rut"] = df["rut_company"].fillna("").astype(str).str.strip()

    # --- Centro ---
    df["centro"] = df["centro"].fillna("").astype(str).str.strip()

    # --- Cupón ---
    df["nombre_cupon"] = df.get("nombre_cupon", pd.Series([""] * len(df)))
    df["nombre_cupon"] = df["nombre_cupon"].fillna("").astype(str).str.upper()

    # --- Montos numéricos ---
    for col in ["venta_neta", "venta_bruta", "valor_envio", "total_final"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # monto_dscto — viene como monto_descuento en el custom
    df["monto_dscto"] = pd.to_numeric(df.get("monto_descuento", 0), errors="coerce").fillna(0).astype(int)

    # last_updated_at se inicializa con la fecha de creación
    df["last_updated_at"] = df["fecha_unificada"]

    # --- Agrupar por order_id: una fila por orden ---
    # Los montos se suman (son por item), el resto se toma el primero
    df = (
        df.groupby("order_id", as_index=False)
        .agg({
            "order_sap":       "first",
            "id_sap_client":   "first",
            "id_destinatario": "first",
            "fecha_unificada": "first",
            "fecha_despacho":  "first",
            "fecha_compra":    "first",
            "hora_compra":     "first",
            "rut":             "first",
            "centro":          "first",
            "direccion":       "first",
            "comuna":          "first",
            "venta_neta":      "sum",
            "venta_bruta":     "sum",
            "valor_envio":     "first",
            "monto_dscto":     "sum",
            "total_final":     "sum",
            "nombre_cupon":    "first",
            "status":          "first",
            "last_updated_at": "first",
        })
    )

    column_order = [
        "order_id", "order_sap",
        "id_sap_client", "id_destinatario",
        "fecha_unificada", "fecha_despacho", "fecha_compra", "hora_compra",
        "rut", "centro", "direccion", "comuna",
        "venta_neta", "venta_bruta", "valor_envio", "monto_dscto", "total_final",
        "nombre_cupon", "status", "last_updated_at",
    ]
    df = df[column_order].where(pd.notnull(df[column_order]), None)

    log.info(f"[transform] transform_orders → {len(df):,} registros")
    return df

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Pendientes
# ---------------------------------------------------------------------------
def transform_clientes_pendientes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "id_sap":                   "id_sap",
        "id_adobe":                 "id_adobe",
        "pros_cli_razon_social":    "razon_social",
        "usu_rut":                  "rut",
        "pros_cli_celular":         "celular",
        "pros_cli_fecha_solicitud": "fecha_solicitud",
        "pros_cli_mail":            "email",
        "pros_cli_contacto":        "contacto",
    })
    df["contacto"] = df["contacto"].str.lower()
    df["id_sap"]   = df["id_sap"].replace("", None)
    df = df.where(pd.notnull(df), None)

    log.info(f"[transform] transform_clientes_pendientes → {len(df):,} registros")
    return df
