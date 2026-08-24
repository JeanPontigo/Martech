"""
load.py  — Pipeline PF B2B
============================
Responsabilidad: persistir datos transformados en la base de datos.

Estándar aplicado (patrón Ariztia):
- State management en DB (pipeline_state) — delta loading real
- pipeline_logs — observabilidad para Looker Studio
- engine.begin() en todos los writes — commits garantizados
- print() reemplazados por logging estructurado
- Imports al tope del módulo (no inline)
- Lógica de negocio de cada función preservada intacta
  (FK checks, deduplicación, métricas derivadas, etc.)

DDL requerido (ejecutar una vez vía data_base.py --setup):
----------------------------------------------------------
  CREATE TABLE IF NOT EXISTS pipeline_state ( ... );
  CREATE TABLE IF NOT EXISTS pipeline_logs  ( ... );
  (ver data_base.py para el DDL completo)
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

log = logging.getLogger("pipeline.load")

DEFAULT_FALLBACK = "2020-01-01 00:00:00"

# ---------------------------------------------------------------------------
# State management — delta loading por entidad
# ---------------------------------------------------------------------------
def get_last_sync(engine, entity: str) -> str:
    """Devuelve el último timestamp sincronizado para la entidad."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT last_sync FROM pipeline_state WHERE entity = :e"),
            {"e": entity},
        ).fetchone()
    return str(row[0]) if row else DEFAULT_FALLBACK


def update_last_sync(engine, entity: str, new_date: str):
    """Upsert del timestamp de sync. engine.begin() garantiza el commit."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_state (entity, last_sync)
                VALUES (:e, :d)
                ON DUPLICATE KEY UPDATE last_sync = :d
            """),
            {"e": entity, "d": new_date},
        )
    log.info(f"[state] {entity} → {new_date}")


def reset_sync(engine, entity: str = None):
    """Resetea el checkpoint de una entidad (o todas si entity=None)."""
    with engine.begin() as conn:
        if entity:
            conn.execute(
                text("DELETE FROM pipeline_state WHERE entity = :e"), {"e": entity}
            )
            log.info(f"[state] Reset de '{entity}'")
        else:
            conn.execute(text("DELETE FROM pipeline_state"))
            log.info("[state] Reset completo de pipeline_state")

# ---------------------------------------------------------------------------
# Observabilidad — pipeline_logs para Looker Studio
# ---------------------------------------------------------------------------
def write_log(
    engine,
    entity: str,
    status: str,
    records_extracted: int = 0,
    duration_sec: float = 0.0,
    dt_from: str = None,
    dt_to: str = None,
    error_msg: str = None,
):
    """
    Registra cada ejecución en pipeline_logs.
    Permite construir en Looker Studio:
      - Última sync exitosa por entidad
      - Volumen extraído en el tiempo
      - Tasa de errores / latencia promedio
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_logs
                    (entity, status, records_extracted, duration_sec,
                     dt_from, dt_to, error_msg)
                VALUES
                    (:entity, :status, :records, :duration,
                     :dt_from, :dt_to, :error_msg)
            """),
            {
                "entity":   entity,
                "status":   status,
                "records":  records_extracted,
                "duration": round(duration_sec, 2),
                "dt_from":  dt_from,
                "dt_to":    dt_to,
                "error_msg": str(error_msg)[:2000] if error_msg else None,
            },
        )
    log.info(
        f"[log] {entity} → {status} | "
        f"{records_extracted:,} registros | {duration_sec:.1f}s"
    )

# ---------------------------------------------------------------------------
# Helper interno
# ---------------------------------------------------------------------------
def _clean_records(records: list) -> list:
    """
    Limpia una lista de dicts antes de enviarla a PyMySQL.
    Convierte float nan, pd.NA, pd.NaT, numpy scalars y strings 'nan' → None.
    Opera sobre los records ya convertidos (post to_dict), evitando
    problemas de dtype que persisten en el DataFrame.
    Compatible con GCP BigQuery — convierte tipos numpy a Python nativos.
    """
    import math
    import numpy as np

    _NAN_STRINGS = {"nan", "none", "null", "nat", "<na>"}

    def _clean(v):
        if v is None:
            return None
        if v is pd.NA or v is pd.NaT:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, str) and v.strip().lower() in _NAN_STRINGS:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return None if math.isnan(float(v)) else float(v)
        if isinstance(v, np.bool_):
            return bool(v)
        return v

    return [{k: _clean(v) for k, v in row.items()} for row in records]

def _disable_fk(conn):
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

def _enable_fk(conn):
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

# ---------------------------------------------------------------------------
# SECCIÓN: Categorías — TRUNCATE + INSERT (catálogo estable)
# ---------------------------------------------------------------------------
def cargar_categorias(df: pd.DataFrame, engine):
    """
    Categorías: TRUNCATE + INSERT.
    Cambian raramente; siempre se recarga el árbol completo.
    """
    df = df.where(pd.notnull(df), None)
    if df.empty:
        log.info("[categorias] Sin datos.")
        return
    with engine.begin() as conn:
        _disable_fk(conn)
        conn.execute(text("DELETE FROM category;"))
        _enable_fk(conn)
    df.to_sql("category", engine, if_exists="append", index=False, chunksize=1000)
    log.info(f"[categorias] {len(df):,} registros cargados.")


# ---------------------------------------------------------------------------
# SECCIÓN: Compañías — UPSERT por PK
# ---------------------------------------------------------------------------
def upsert_companias(df: pd.DataFrame, engine):
    """
    Upsert de compañías por company_id.
    ON DUPLICATE KEY UPDATE absorbe nuevas y actualizadas sin error.
    """
    if df.empty:
        log.info("[companias] Sin datos.")
        return

    records = _clean_records(df.to_dict("records"))
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO company (
                    company_id, company_name, oficina_venta, codigo_oficina,
                    company_code, city, region, company_email, status, rut_company
                ) VALUES (
                    :company_id, :company_name, :oficina_venta, :codigo_oficina,
                    :company_code, :city, :region, :company_email, :status, :rut_company
                )
                ON DUPLICATE KEY UPDATE
                    company_name   = VALUES(company_name),
                    status         = VALUES(status),
                    company_email  = VALUES(company_email),
                    city           = VALUES(city),
                    region         = VALUES(region),
                    oficina_venta  = VALUES(oficina_venta),
                    rut_company    = VALUES(rut_company),
                    codigo_oficina = VALUES(codigo_oficina),
                    company_code   = VALUES(company_code)
            """),
            records,
        )
    log.info(f"[companias] {len(records):,} registros upserted.")


# ---------------------------------------------------------------------------
# SECCIÓN: Clientes — UPSERT por PK
# ---------------------------------------------------------------------------
def upsert_clientes(df: pd.DataFrame, engine):
    """
    Upsert de clientes por client_id.
    - Valida FK company_id contra tabla company
    - Inserta nuevos y actualiza campos de clientes existentes
    """
    if df.empty:
        log.info("[clientes] Sin datos.")
        return

    # Validar FK company_id
    with engine.connect() as conn:
        valid_companies = {
            row[0] for row in
            conn.execute(text("SELECT company_id FROM company")).fetchall()
        }

    df["company_id"] = df["company_id"].apply(
        lambda x: x if x in valid_companies else None
    )

    records = _clean_records(df.to_dict("records"))
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO client (
                    client_id, group_id, group_name, client_rut,
                    created_at, updated_at,
                    client_email, firstname, lastname,
                    company_id, company_rut
                ) VALUES (
                    :client_id, :group_id, :group_name, :client_rut,
                    :created_at, :updated_at,
                    :client_email, :firstname, :lastname,
                    :company_id, :company_rut
                )
                ON DUPLICATE KEY UPDATE
                    group_id     = VALUES(group_id),
                    group_name   = VALUES(group_name),
                    updated_at   = VALUES(updated_at),
                    client_email = VALUES(client_email),
                    firstname    = VALUES(firstname),
                    lastname     = VALUES(lastname),
                    company_id   = VALUES(company_id),
                    company_rut  = VALUES(company_rut)
            """),
            records,
        )
    log.info(f"[clientes] {len(records):,} registros upserted.")


# ---------------------------------------------------------------------------
# SECCIÓN: Productos — UPSERT por PK (sku)
# ---------------------------------------------------------------------------
def upsert_productos(df: pd.DataFrame, engine):
    """
    UPSERT bulk de productos por PK (sku).
    ON DUPLICATE KEY UPDATE absorbe duplicados sin error.
    """
    if df.empty:
        log.info("[productos] Sin datos.")
        return

    records = _clean_records(df.to_dict("records"))
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO product (
                    sku, sku_name, product_type, availability,
                    created_at, updated_at,
                    peso_promedio_kg, unidad_x_producto,
                    marca, category_id, category, sub_category, image_sku
                ) VALUES (
                    :sku, :sku_name, :product_type, :availability,
                    :created_at, :updated_at,
                    :peso_promedio_kg, :unidad_x_producto,
                    :marca, :category_id, :category, :sub_category, :image_sku
                )
                ON DUPLICATE KEY UPDATE
                    sku_name          = VALUES(sku_name),
                    availability      = VALUES(availability),
                    updated_at        = VALUES(updated_at),
                    peso_promedio_kg  = VALUES(peso_promedio_kg),
                    unidad_x_producto = VALUES(unidad_x_producto),
                    marca             = VALUES(marca),
                    category_id       = VALUES(category_id),
                    category          = VALUES(category),
                    sub_category      = VALUES(sub_category),
                    image_sku         = VALUES(image_sku)
            """),
            records,
        )
    log.info(f"[productos] {len(records):,} registros upserted.")


# ---------------------------------------------------------------------------
# SECCIÓN: Órdenes — UPSERT con status tracking
# ---------------------------------------------------------------------------
def upsert_orders(df: pd.DataFrame, engine):
    """
    Upsert de órdenes con:
    - Validación de FK client_id y company_id
    - Estado tracking: INSERT nuevas + UPDATE estado de existentes
    """
    if df.empty:
        log.info("[orders] DataFrame vacío. Nada que insertar.")
        return

    # FKs válidas
    with engine.connect() as conn:
        valid_clients   = {r[0] for r in conn.execute(text("SELECT client_id FROM client")).fetchall()}
        valid_companies = {r[0] for r in conn.execute(text("SELECT company_id FROM company")).fetchall()}

    df["client_id"]  = df["client_id"].apply(lambda x: x if x in valid_clients else None)
    df["company_id"] = df["company_id"].apply(lambda x: x if x in valid_companies else None)

    records = _clean_records(df.to_dict("records"))
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO orders (
                    order_id, client_id, company_id,
                    created_at, updated_at, estado,
                    total, subtotal, payment_method,
                    descuento, envio, fecha_envio,
                    ciudad, region
                ) VALUES (
                    :order_id, :client_id, :company_id,
                    :created_at, :updated_at, :estado,
                    :total, :subtotal, :payment_method,
                    :descuento, :envio, :fecha_envio,
                    :ciudad, :region
                )
                ON DUPLICATE KEY UPDATE
                    updated_at     = VALUES(updated_at),
                    estado         = VALUES(estado),
                    total          = VALUES(total),
                    subtotal       = VALUES(subtotal),
                    payment_method = VALUES(payment_method),
                    descuento      = VALUES(descuento),
                    envio          = VALUES(envio),
                    fecha_envio    = VALUES(fecha_envio),
                    ciudad         = VALUES(ciudad),
                    region         = VALUES(region)
            """),
            records,
        )
    log.info(f"[orders] {len(records):,} registros upserted.")


def actualizar_estado_ordenes(df: pd.DataFrame, engine):
    """
    Actualiza SOLO el campo estado de órdenes que ya existen en DB.
    Se usa con órdenes filtradas por updated_at.
    """
    if df.empty:
        return

    with engine.connect() as conn:
        existing_ids = {
            row[0] for row in
            conn.execute(text("SELECT order_id FROM orders")).fetchall()
        }

    actualizadas = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            order_id = row.get("order_id")
            estado   = row.get("estado")
            if order_id in existing_ids and estado:
                conn.execute(
                    text("""
                        UPDATE orders
                        SET estado = :estado, updated_at = NOW()
                        WHERE order_id = :order_id AND estado != :estado
                    """),
                    {"estado": estado, "order_id": order_id},
                )
                actualizadas += 1

    log.info(f"[orders] Estado actualizado en {actualizadas:,} órdenes existentes.")


# ---------------------------------------------------------------------------
# SECCIÓN: Order Items — UPSERT con FK check
# ---------------------------------------------------------------------------
def upsert_order_items(df: pd.DataFrame, engine):
    """
    Upsert de líneas de orden con:
    - Validación de FK order_id contra orders
    - Validación de FK sku contra product
    - Upsert por order_id + sku (PK lógica de la tabla)
    """
    if df.empty:
        log.info("[order_items] Sin datos.")
        return

    with engine.connect() as conn:
        valid_orders = {
            row[0] for row in
            conn.execute(text("SELECT order_id FROM orders")).fetchall()
        }
        valid_skus = {
            row[0] for row in
            conn.execute(text("SELECT sku FROM product")).fetchall()
        }

    before = len(df)
    df = df[df["order_id"].isin(valid_orders) & df["sku"].isin(valid_skus)]
    dropped = before - len(df)
    if dropped:
        log.warning(f"[order_items] {dropped} ítems descartados (FK inválida).")

    if df.empty:
        log.info("[order_items] Sin registros válidos tras FK check.")
        return

    records = _clean_records(df.to_dict("records"))
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO order_item (
                    order_id, sku, sku_name,
                    sku_qty, sku_value, sku_total_value,
                    base_price, original_price, en_oferta
                ) VALUES (
                    :order_id, :sku, :sku_name,
                    :sku_qty, :sku_value, :sku_total_value,
                    :base_price, :original_price, :en_oferta
                )
                ON DUPLICATE KEY UPDATE
                    sku_qty         = VALUES(sku_qty),
                    sku_value       = VALUES(sku_value),
                    sku_total_value = VALUES(sku_total_value),
                    base_price      = VALUES(base_price),
                    original_price  = VALUES(original_price),
                    en_oferta       = VALUES(en_oferta)
            """),
            records,
        )
    log.info(f"[order_items] {len(records):,} líneas upserted.")


# ---------------------------------------------------------------------------
# SECCIÓN: Campos derivados en client — actualizados al final de cada corrida
# ---------------------------------------------------------------------------
def actualizar_metricas_derivadas(engine):
    """
    Actualiza los tres campos calculados en la tabla client:
    - fecha_ultima_compra → MAX(created_at) de sus órdenes
    - compras_totales     → COUNT(*) de sus órdenes
    - ticket_promedio     → AVG(total) de sus órdenes
    El historial de ejecuciones queda en pipeline_logs, no en una tabla metrics separada.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE client c
            JOIN (
                SELECT client_id, MAX(created_at) AS fecha_ultima
                FROM orders GROUP BY client_id
            ) o ON c.client_id = o.client_id
            SET c.fecha_ultima_compra = o.fecha_ultima;
        """))
        log.info("[client] fecha_ultima_compra actualizada.")

        conn.execute(text("""
            UPDATE client c
            JOIN (
                SELECT client_id, COUNT(*) AS total
                FROM orders GROUP BY client_id
            ) o ON c.client_id = o.client_id
            SET c.compras_totales = o.total;
        """))
        log.info("[client] compras_totales actualizado.")

        conn.execute(text("""
            UPDATE client c
            JOIN (
                SELECT client_id,
                       IFNULL(SUM(total) / NULLIF(COUNT(*), 0), 0) AS ticket
                FROM orders GROUP BY client_id
            ) o ON c.client_id = o.client_id
            SET c.ticket_promedio = o.ticket;
        """))
        log.info("[client] ticket_promedio actualizado.")
