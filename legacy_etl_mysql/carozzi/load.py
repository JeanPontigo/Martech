"""
load.py
=======
Responsabilidad: persistir datos transformados en la base de datos.

Cambios respecto al original:
- pipeline_state  → delta loading real por entidad (reemplaza max(updated_at) en Excel)
- pipeline_logs   → observabilidad para Looker Studio (reemplaza tabla metricas)
- Bulk upsert con ON DUPLICATE KEY UPDATE (reemplaza merge + insert + update separados)
- engine.begin()  en todos los writes — commits garantizados
- FK check en orders_items preservado (product_id nullificado si no existe en product)
- post-load de last_order_date / total_orders / is_first_order preservado
- Eliminadas: validacion_salida, actualizar_excel, toda referencia a metricas
- Logging estructurado (sin print ni tqdm)

DDL requerido (ejecutar una vez vía bbdd.py o manualmente):
-------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS pipeline_state (
      entity     VARCHAR(64) PRIMARY KEY,
      last_sync  DATETIME    NOT NULL,
      updated_at DATETIME    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS pipeline_logs (
      id                INT AUTO_INCREMENT PRIMARY KEY,
      entity            VARCHAR(64)                      NOT NULL,
      status            ENUM('success','error','empty')  NOT NULL,
      records_extracted INT                              DEFAULT 0,
      duration_sec      FLOAT                            DEFAULT 0,
      dt_from           DATETIME,
      dt_to             DATETIME,
      error_msg         TEXT,
      executed_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_entity_executed (entity, executed_at)
  );
"""

import logging
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import text

log = logging.getLogger("pipeline.load")

DEFAULT_FALLBACK = "2021-01-01 00:00:00"


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
    """Upsert del timestamp de sync."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_state (entity, last_sync)
                VALUES (:e, :d)
                ON DUPLICATE KEY UPDATE last_sync = :d
            """),
            {"e": entity, "d": new_date},
        )
    log.info("[state] %s → %s", entity, new_date)


def reset_sync(engine, entity: str = None):
    """Resetea el checkpoint de una entidad (o todas si entity=None)."""
    with engine.begin() as conn:
        if entity:
            conn.execute(text("DELETE FROM pipeline_state WHERE entity = :e"), {"e": entity})
            log.info("[state] Reset de '%s'", entity)
        else:
            conn.execute(text("DELETE FROM pipeline_state"))
            log.info("[state] Reset completo de pipeline_state")


# ---------------------------------------------------------------------------
# Observabilidad — pipeline_logs
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
                "entity":    entity,
                "status":    status,
                "records":   records_extracted,
                "duration":  round(duration_sec, 2),
                "dt_from":   dt_from,
                "dt_to":     dt_to,
                "error_msg": str(error_msg)[:2000] if error_msg else None,
            },
        )
    log.info("[log] %s → %s | %s registros | %.1fs", entity, status, f"{records_extracted:,}", duration_sec)


# ---------------------------------------------------------------------------
# Helper interno
# ---------------------------------------------------------------------------
def _clean_records(records: list) -> list:
    """
    Limpia una lista de dicts antes de enviarla a PyMySQL.
    Convierte float nan, pd.NA, pd.NaT, numpy scalars y strings 'nan' → None.
    Opera sobre los records ya convertidos (post to_dict), evitando
    problemas de dtype que persisten en el DataFrame.
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


# ---------------------------------------------------------------------------
# SECCIÓN: Category
# ---------------------------------------------------------------------------
def load_category(df: pd.DataFrame, engine):
    if df.empty:
        log.info("[category] DataFrame vacío.")
        return

    records = _clean_records(df.to_dict("records"))
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO category (category_id, parent_id, category_name, is_active, product_count)
                VALUES (:category_id, :parent_id, :category_name, :is_active, :product_count)
                ON DUPLICATE KEY UPDATE
                    parent_id     = VALUES(parent_id),
                    category_name = VALUES(category_name),
                    is_active     = VALUES(is_active),
                    product_count = VALUES(product_count)
            """),
            records,
        )
    log.info("[category] %s registros upserted.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def load_productos(df: pd.DataFrame, engine):
    if df.empty:
        log.info("[product] DataFrame vacío.")
        return

    cols = [
        "entity_id", "sku", "sku_name", "price", "status", "visibility",
        "type_id", "created_at", "updated_at", "linked_product_type",
        "product_image", "peso_promedio_kg", "marca",
        "category_id", "category_name", "sub_category", "position",
    ]
    cols    = [c for c in cols if c in df.columns]
    records = _clean_records(df[cols].to_dict("records"))

    cols_sql    = ", ".join(cols)
    params_sql  = ", ".join(f":{c}" for c in cols)
    update_sql  = ", ".join(f"{c} = VALUES({c})" for c in cols if c != "entity_id")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO product ({cols_sql})
                VALUES ({params_sql})
                ON DUPLICATE KEY UPDATE {update_sql}
            """),
            records,
        )
    log.info("[product] %s registros upserted.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# SECCIÓN: Customers
# ---------------------------------------------------------------------------
def load_customers(df: pd.DataFrame, engine):
    if df.empty:
        log.info("[customers] DataFrame vacío.")
        return

    cols = [
        "customer_id", "customer_group_id", "customer_group_name",
        "created_at", "updated_at", "customer_email",
        "customer_firstname", "customer_lastname",
        "company", "rut", "region", "city",
        "last_order_date", "total_orders", "is_first_order",
    ]
    cols    = [c for c in cols if c in df.columns]
    records = _clean_records(df[cols].to_dict("records"))

    cols_sql   = ", ".join(cols)
    params_sql = ", ".join(f":{c}" for c in cols)
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in cols if c != "customer_id")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO customers ({cols_sql})
                VALUES ({params_sql})
                ON DUPLICATE KEY UPDATE {update_sql}
            """),
            records,
        )
    log.info("[customers] %s registros upserted.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# SECCIÓN: Orders
# ---------------------------------------------------------------------------
def actualizar_status_ordenes(df: pd.DataFrame, engine):
    """
    Actualiza SOLO el campo status de órdenes que ya existen en la DB.
    Se usa con órdenes filtradas por updated_at — órdenes viejas que
    cambiaron de status recientemente.
    No inserta órdenes nuevas.
    """
    if df.empty:
        return

    with engine.connect() as conn:
        existing_ids = {
            row[0] for row in conn.execute(
                text("SELECT order_id FROM orders")
            ).fetchall()
        }

    actualizadas = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            order_id = row.get("order_id")
            status   = row.get("status")
            if order_id in existing_ids and status:
                conn.execute(
                    text("""
                        UPDATE orders
                        SET status = :status, updated_at = :updated_at
                        WHERE order_id = :order_id AND status != :status
                    """),
                    {"status": status, "order_id": order_id, "updated_at": row.get("updated_at")},
                )
                actualizadas += 1

    log.info("[orders] Status actualizado en %s órdenes existentes.", f"{actualizadas:,}")


def load_orders(df: pd.DataFrame, engine):
    """
    Upsert de órdenes.
    - customer_id se enriquece cruzando por email cuando viene nulo desde Magento
      (comportamiento conocido de la API para ciertos tipos de órdenes)
    - customer_id que no existe en customers queda como NULL
    """
    if df.empty:
        log.info("[orders] DataFrame vacío.")
        return

    with engine.connect() as conn:
        # Mapa email → customer_id para enriquecer órdenes sin ID
        email_to_id = {
            row[0].lower(): row[1]
            for row in conn.execute(
                text("SELECT customer_email, customer_id FROM customers WHERE customer_email IS NOT NULL")
            ).fetchall()
        }

    # Enriquecer customer_id nulo cruzando por email
    mask_nulos = df["customer_id"].isna()
    if mask_nulos.any():
        df["customer_id"] = df["customer_id"].astype(object)
        df.loc[mask_nulos, "customer_id"] = df.loc[mask_nulos, "customer_email"].apply(
            lambda email: email_to_id.get(email.lower()) if isinstance(email, str) else None
        )
        enriquecidos = df["customer_id"].notna().sum() - (~mask_nulos).sum()
        log.info("[orders] %s customer_id enriquecidos por email.", f"{max(enriquecidos, 0):,}")

    cols = [
        "order_id", "base_grand_total", "created_at", "updated_at",
        "grand_total", "shipping_amount", "customer_id", "customer_email",
        "state", "status", "subtotal", "discount_amount",
        "order_city", "order_region", "peso_promedio_kg",
        "method", "total_item_count", "total_qty_ordered",
    ]
    cols    = [c for c in cols if c in df.columns]
    records = _clean_records(df[cols].to_dict("records"))

    cols_sql   = ", ".join(cols)
    params_sql = ", ".join(f":{c}" for c in cols)
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in cols if c != "order_id")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO orders ({cols_sql})
                VALUES ({params_sql})
                ON DUPLICATE KEY UPDATE {update_sql}
            """),
            records,
        )
    log.info("[orders] %s registros upserted.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# SECCIÓN: Order Items
# ---------------------------------------------------------------------------
def load_orders_items(df: pd.DataFrame, engine):
    """
    Upsert de items con FK check: product_id se nullifica si no existe
    en la tabla product — evita error de FK sin interrumpir la carga.
    """
    if df.empty:
        log.info("[orders_items] DataFrame vacío.")
        return

    with engine.connect() as conn:
        valid_product_ids = {
            row[0] for row in conn.execute(text("SELECT entity_id FROM product")).fetchall()
        }

    invalidos = ~df["product_id"].isin(valid_product_ids)
    if invalidos.any():
        log.info("[orders_items] %s product_id inválidos → NULL", f"{invalidos.sum():,}")
        df.loc[invalidos, "product_id"] = None

    cols = [
        "item_id", "order_id", "created_at", "updated_at",
        "product_id", "product_type", "original_price",
        "qty_ordered", "qty_invoiced", "qty_shipped", "sku",
    ]
    cols    = [c for c in cols if c in df.columns]
    records = _clean_records(df[cols].to_dict("records"))

    cols_sql   = ", ".join(cols)
    params_sql = ", ".join(f":{c}" for c in cols)
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in cols if c != "item_id")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO orders_items ({cols_sql})
                VALUES ({params_sql})
                ON DUPLICATE KEY UPDATE {update_sql}
            """),
            records,
        )
    log.info("[orders_items] %s registros upserted.", f"{len(records):,}")


# ---------------------------------------------------------------------------
# SECCIÓN: Post-load — actualizar columnas derivadas en customers
# ---------------------------------------------------------------------------
def actualizar_datos_customers(engine):
    """
    Actualiza last_order_date, total_orders e is_first_order en customers
    cruzando con la tabla orders. Preservado del original (bbdd.py).
    Se ejecuta una vez al final del pipeline completo.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE customers c
            JOIN (
                SELECT customer_id, MAX(created_at) AS fecha_ultima
                FROM orders
                GROUP BY customer_id
            ) o ON c.customer_id = o.customer_id
            LEFT JOIN (
                SELECT LOWER(customer_email) AS customer_email, MAX(created_at) AS fecha_ultima
                FROM orders
                GROUP BY LOWER(customer_email)
            ) w ON w.customer_email <=> LOWER(c.customer_email)
            SET c.last_order_date = COALESCE(o.fecha_ultima, w.fecha_ultima)
        """))
        log.info("[customers] last_order_date actualizado.")

        conn.execute(text("""
            UPDATE customers c
            JOIN (
                SELECT customer_id, COUNT(*) AS total
                FROM orders
                GROUP BY customer_id
            ) o ON c.customer_id = o.customer_id
            LEFT JOIN (
                SELECT LOWER(customer_email) AS customer_email, COUNT(*) AS total
                FROM orders
                GROUP BY LOWER(customer_email)
            ) w ON w.customer_email <=> LOWER(c.customer_email)
            SET c.total_orders = COALESCE(o.total, w.total)
        """))
        log.info("[customers] total_orders actualizado.")

        conn.execute(text("""
            UPDATE customers c
            SET c.is_first_order = CASE
                WHEN c.total_orders = 1 THEN 1
                WHEN c.total_orders > 1 THEN 0
                ELSE 0
            END
        """))
        log.info("[customers] is_first_order actualizado.")
