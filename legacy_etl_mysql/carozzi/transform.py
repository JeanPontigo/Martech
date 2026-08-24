"""
transform.py
============
Responsabilidad: limpiar, normalizar y estructurar los DataFrames
extraídos antes de cargarlos en la base de datos.

Cambios respecto al original:
- Todas las funciones reciben DataFrames como parámetro (no leen desde Excel)
- Todas las funciones retornan DataFrames (no escriben a CSV)
- normalizar_rut preservada intacta (lógica de negocio crítica)
- Lógica de categorías/subcategorías preservada intacta
- Logging estructurado (sin print ni tqdm)
"""

import re
import os
import logging

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("pipeline.transform")


# ---------------------------------------------------------------------------
# Helper: normalización de RUT
# ---------------------------------------------------------------------------
def normalizar_rut(series: pd.Series, estado: str) -> pd.Series:
    """
    Normaliza RUTs chilenos a formato xx.xxx.xxx-X.
    estado='general'  → normalización estricta, inválidos → '55.555.555-5'
    estado='compañia' → normalización tolerante, ignora no-RUTs silenciosamente
    Lógica preservada intacta del original.
    """
    factura = series.astype("string").str.strip()

    if estado == "general":
        factura = factura.replace(r"\s+", "", regex=True)
        factura = factura.replace(r"[^0-9Kk\.\-]", "", regex=True)

        if factura.str.contains(r"-k$", na=False).any():
            factura = factura.str.replace(r"-k$", "-K", regex=True)

        if factura.str.contains(r"^\d{7,9}[\dkK]$", na=False).any():
            def crear_nuevo_formato(x):
                if x is None or not re.fullmatch(r"\d{7,9}[\dkK]", x):
                    return x
                body, ver = x[:-1], x[-1].upper()
                body = f"{int(body):,}".replace(",", ".")
                return f"{body}-{ver}"
            factura = factura.apply(crear_nuevo_formato)

        if factura.str.contains(r"^\d{7,9}-[\dkK]$", na=False).any():
            def ordenar_nuevo_formato(x):
                if x is None or not re.fullmatch(r"\d{7,9}-[\dkK]", x):
                    return x
                body, ver = x.split("-")
                body = f"{int(body):,}".replace(",", ".")
                return f"{body}-{ver.upper()}"
            factura = factura.apply(ordenar_nuevo_formato)

        if factura.str.contains(r"\d{3}\.\d{3}-[\dkK]", na=False).any():
            def formato_incompleto(x):
                if x is None or not re.fullmatch(r"\d{3}\.\d{3}-[\dkK]", x):
                    return x
                alone = str(x).strip()
                if alone.count(".") == 1 and re.fullmatch(r"\d{3}\.\d{3}-[\dkK]", alone):
                    body, ver = alone.split("-")
                    digital = body.replace(".", "")
                    return f"{digital[0]}.{digital[1:4]}.{digital[4:6]}-{ver.upper()}"
                return alone
            factura = factura.apply(formato_incompleto)

        valido = factura.str.match(
            r"(?:\d{1,3}(?:\.\d{3}){2}|\d{2}\.\d{3}\.\d{2}|\d\.\d{3}\.\d{2})-[\dK]$",
            na=False,
        )
        return factura.where(valido, other="55.555.555-5")

    elif estado == "compañia":
        evitar = factura.notna() & (factura != "") & factura.str.contains(r"\d", na=False)
        conteo_rut = factura.str.count(r"\d") >= 7
        fecha      = factura.str.match(r"^.*\d{1,2}(/|-)\d{1,2}(/|-)\d{2,4}.*$", na=False)
        telefono   = factura.str.match(r"^\+569\d{8}$", na=False)
        mascara    = evitar & conteo_rut & (~fecha) & (~telefono)

        validos = factura[mascara]
        validos = validos.replace(r"\s+", "", regex=True)
        validos = validos.replace(r"[^0-9Kk\.\-]", "", regex=True)
        validos = validos.replace(r"[kK](?=[^-\n]*-)", "", regex=True)
        validos = validos.replace(r"[kK](?=.{2,}$)", "", regex=True)

        if validos.str.contains(r"-k$", na=False).any():
            validos = validos.str.replace(r"-k$", "-K", regex=True)

        if validos.str.contains(r"^\d{7,9}[\dkK]$", na=False).any():
            def crear_nuevo_formato(x):
                if x is None or not re.fullmatch(r"\d{7,9}[\dkK]", x):
                    return x
                body, ver = x[:-1], x[-1].upper()
                body = f"{int(body):,}".replace(",", ".")
                return f"{body}-{ver}"
            validos = validos.apply(crear_nuevo_formato)

        if validos.str.contains(r"^\d{7,9}-[\dkK]$", na=False).any():
            def ordenar_nuevo_formato(x):
                if x is None or not re.fullmatch(r"\d{7,9}-[\dkK]", x):
                    return x
                body, ver = x.split("-")
                body = f"{int(body):,}".replace(",", ".")
                return f"{body}-{ver.upper()}"
            validos = validos.apply(ordenar_nuevo_formato)

        if validos.str.contains(r"\d{3}\.\d{3}-[\dkK]", na=False).any():
            def formato_incompleto(x):
                if x is None or not re.fullmatch(r"\d{3}\.\d{3}-[\dkK]", x):
                    return x
                alone = str(x).strip()
                if alone.count(".") == 1 and re.fullmatch(r"\d{3}\.\d{3}-[\dkK]", alone):
                    body, ver = alone.split("-")
                    digital = body.replace(".", "")
                    return f"{digital[0]}.{digital[1:4]}.{digital[4:6]}-{ver.upper()}"
                return alone
            validos = validos.apply(formato_incompleto)

        if validos.str.contains(r"^\d{2}\.\d{3}\.\d{4}$", na=False).any():
            def terminar_formato(x):
                if x is None or not re.fullmatch(r"^\d{2}\.\d{3}\.\d{4}$", x):
                    return x
                one, two, three = x.split(".")
                sep = re.sub(r"^(\d{3})(\d)$", r"\1-\2", three)
                return f"{one}.{two}.{sep}"
            validos = validos.apply(terminar_formato)

        esquema = validos.str.match(
            r"(?:\d{1,3}(?:\.\d{3}){2}|\d{2}\.\d{3}\.\d{2}|\d\.\d{3}\.\d{2})-[\dK]$",
            na=False,
        )
        factura.loc[validos.index] = validos
        return factura

    return series


# ---------------------------------------------------------------------------
# Timestamps se mantienen en UTC — mismo formato que Magento y compatible con GCP


# ---------------------------------------------------------------------------
# SECCIÓN: Category
# ---------------------------------------------------------------------------
def transform_category(df: pd.DataFrame, for_load: bool = False) -> pd.DataFrame:
    """
    Transforma el árbol de categorías.
    - for_load=False (default) → preserva columnas level/path/tipo
      necesarias para enriquecer productos en transform_productos()
    - for_load=True → elimina esas columnas antes de insertar en DB
    """
    df = df.copy()
    df = df.sort_values("category_id", ascending=True, kind="mergesort")
    df["is_active"] = (
        df["is_active"]
        .astype(str).str.strip().str.upper()
        .map({"VERDADERO": True, "FALSO": False, "TRUE": True, "FALSE": False})
    )
    df["is_active"] = pd.to_numeric(df["is_active"], errors="coerce").astype("Int64")
    if for_load:
        df = df.drop(columns=["level", "path", "tipo"], errors="ignore")
    log.info("[transform] category → %s registros", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def transform_productos(
    df_productos: pd.DataFrame,
    df_category: pd.DataFrame,
    df_marcas: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transforma productos enriqueciendo con categorías y marcas.
    Recibe los tres DataFrames en memoria — sin lectura de Excel.
    Lógica de selección de categoría/subcategoría preservada intacta.
    """
    productos = df_productos.copy().astype(str)
    categoria = df_category.copy()

    # Mapa parent_id y level por category_id
    parent_day  = dict(zip(categoria["category_id"].astype(int), categoria["parent_id"].astype(int)))
    niveles_cat = dict(zip(categoria["category_id"].astype(int), categoria["level"].astype(int)))

    def epica(ids):
        cats     = [i for i in ids if parent_day.get(i) == 2]
        children = {}
        for i in ids:
            p = parent_day.get(i)
            if p is not None:
                children.setdefault(p, []).append(i)
        mouse  = next((c for c in cats if c in children), cats[0] if cats else None)
        subcat = next((i for i in ids if mouse is not None and parent_day.get(i) == mouse), None)
        return mouse, subcat

    def seleccion(row):
        ids = list(map(int, re.findall(r"\d+", str(row["category_id"]))))
        if not ids:
            return pd.Series([None, 0, None], index=["category_id", "position", "sub_category"])
        pos_nums = (
            list(map(int, re.findall(r"-?\d+", str(row.get("position", "")))))
            if "position" in row else []
        )
        aligned = len(pos_nums) == len(ids)
        mouse, subcat = epica(ids)

        if mouse is None:
            mouse  = ids[0] if len(ids) == 1 else max(ids, key=lambda i: niveles_cat.get(i, 1))
            subcat = next((i for i in ids if parent_day.get(i) == mouse), None)

        pos = pos_nums[ids.index(mouse)] if aligned else 0
        return pd.Series(
            [int(mouse), int(pos), int(subcat) if subcat is not None else None],
            index=["category_id", "position", "sub_category"],
        )

    productos[["category_id", "position", "sub_category"]] = productos.apply(seleccion, axis=1)

    # Nombres de categoría y subcategoría
    productos["category_id"]   = pd.to_numeric(productos["category_id"], errors="coerce")
    id_to_name                 = dict(zip(categoria["category_id"].dropna().astype(int), categoria["category_name"]))
    productos["category_name"] = productos["category_id"].map(id_to_name)
    productos["sub_category"]  = productos["sub_category"].map(id_to_name)

    # Sub-categoría de prueba / sampling
    prueba = productos["sku_name"].astype(str).str.contains(r"^\s*Sampling|\s*test", na=False, case=False)
    productos["sub_category"] = productos["sub_category"].astype(str).replace("nan", "").str.strip().str.lower()
    productos.loc[prueba, "sub_category"] = "DATOS DE PRUEBA"

    # Marcas — enriquecer desde DataFrame en memoria
    marcas = df_marcas.copy()
    productos["marca"] = productos["marca"].astype(str).str.strip()
    marcas["value"]    = marcas["value"].astype(str).str.strip()
    marcas["label"]    = marcas["label"].astype(str).str.strip()
    productos = productos.merge(marcas.rename(columns={"value": "marca"}), on="marca", how="left")
    productos["marca"] = productos["label"].fillna(productos["marca"])
    productos = productos.drop(columns=["label"], errors="ignore")

    # URL de imagen
    base_url = os.getenv("MC_URL", "").rstrip("/") + "/media/catalog/product/"
    productos["product_image"] = base_url + productos["product_image"].astype(str).str.lstrip("/")

    # Timezone y tipos numéricos
    productos["price"] = pd.to_numeric(productos["price"], errors="coerce").fillna(0).astype(float)
    productos["marca"] = pd.to_numeric(productos["marca"], errors="coerce")

    for col in ["product_image", "category_id", "position", "linked_product_type"]:
        if col in productos.columns:
            productos[col] = productos[col].replace(r"^\s*$", None, regex=True)

    log.info("[transform] productos → %s registros", f"{len(productos):,}")
    return productos


# ---------------------------------------------------------------------------
# SECCIÓN: Customers
# ---------------------------------------------------------------------------
def transform_customers(
    df_customers: pd.DataFrame,
    df_groups: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transforma clientes normalizando RUTs y enriqueciendo con grupos.
    Recibe ambos DataFrames en memoria — sin lectura de Excel.
    """
    df  = df_customers.copy()
    df2 = df_groups.copy()

    df["rut"]     = normalizar_rut(df["rut"].astype("string"),     estado="general")
    df["company"] = normalizar_rut(df["company"].astype("string"), estado="compañia")

    df2["code"] = df2["code"].str.strip()
    grupo_map   = dict(zip(df2["customer_group_id"], df2["code"]))
    df["customer_group_name"] = df["customer_group_name"].map(grupo_map)
    df = df.drop_duplicates(subset=["customer_id"], keep="last")

    df["last_order_date"] = None
    df["total_orders"]    = None
    df["is_first_order"]  = None

    df = df.where(pd.notnull(df), None)

    log.info("[transform] customers → %s registros", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Orders
# ---------------------------------------------------------------------------
def transform_orders(df_orders: pd.DataFrame) -> pd.DataFrame:
    df = df_orders.copy()

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").where(
        lambda x: x != 0, other=None
    )

    for col in ["base_grand_total", "grand_total", "shipping_amount", "subtotal", "discount_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)

    df = df.where(pd.notnull(df), None)
    log.info("[transform] orders → %s registros", f"{len(df):,}")
    return df


# ---------------------------------------------------------------------------
# SECCIÓN: Order Items
# ---------------------------------------------------------------------------
def transform_orders_items(df_items: pd.DataFrame) -> pd.DataFrame:
    df = df_items.copy()

    df["sku"]          = df["sku"].astype(str).str.strip()
    df["product_type"] = df["product_type"].astype(str).str.strip()

    df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce").fillna(0).astype(float)
    df = df.where(pd.notnull(df), None)

    log.info("[transform] order_items → %s registros", f"{len(df):,}")
    return df
