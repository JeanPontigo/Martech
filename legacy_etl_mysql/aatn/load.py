"""
load.py
=======
Responsabilidad: persistir datos transformados en la base de datos.

Mejoras respecto al original:
- State management en DB (pipeline_state) — delta loading real
- pipeline_logs — observabilidad para Looker Studio
- engine.begin() en todos los writes — commits garantizados
- print() reemplazados por logging estructurado
- Imports al tope del módulo (no inline)
- Lógica de negocio de cada función preservada intacta
  (FK checks, status tracking, RUT conflicts, etc.)

DDL requerido (ejecutar una vez):
----------------------------------
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

import numpy as np
import pandas as pd
from sqlalchemy import text

from utils import normalizar_rut

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
            conn.execute(text("DELETE FROM pipeline_state WHERE entity = :e"), {"e": entity})
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
      - Horas sin datos (NOW() - MAX(executed_at))
      - Volumen extraído en el tiempo
      - Tasa de errores
      - Latencia promedio
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
                "entity":    entity,
                "status":    status,
                "records":   records_extracted,
                "duration":  round(duration_sec, 2),
                "dt_from":   dt_from,
                "dt_to":     dt_to,
                "error_msg": str(error_msg)[:2000] if error_msg else None,
            },
        )
    log.info(f"[log] {entity} → {status} | {records_extracted:,} registros | {duration_sec:.1f}s")

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

# ---------------------------------------------------------------------------
# SECCIÓN: Order Info
# ---------------------------------------------------------------------------
def cargar_info_orders(df: pd.DataFrame, table_name: str, engine):
    """
    Carga info_orders:
    - Valida que order_id exista en ariztia_orders
    - Inserta solo registros nuevos (no duplicados)
    """
    with engine.connect() as conn:
        valid_orders = pd.read_sql(
            text("SELECT DISTINCT order_id FROM ariztia_orders"), conn
        )["order_id"].tolist()

        df_filtered = df[df["order_id"].isin(valid_orders)]
        if df_filtered.empty:
            log.info("[cargar_info_orders] Sin datos válidos para cargar.")
            return

        existing_orders = pd.read_sql(
            text(f"SELECT DISTINCT order_id FROM {table_name}"), conn
        )["order_id"].tolist()

        df_to_insert = df_filtered[~df_filtered["order_id"].isin(existing_orders)]
        if df_to_insert.empty:
            log.info("[cargar_info_orders] Sin datos nuevos para cargar.")
            return

    df_to_insert.to_sql(
        name=table_name, con=engine, if_exists="append", index=False, chunksize=1000
    )
    log.info(f"[cargar_info_orders] {len(df_to_insert):,} registros insertados en {table_name}.")

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Aprobados
# ---------------------------------------------------------------------------
def insertar_o_actualizar_clientes(df: pd.DataFrame, table_name: str, engine, verbose: bool = False):
    """
    Upsert de clientes aprobados con:
    - Protección de FK en usu_rut (no actualiza si cambió y hay FK activa)
    - Actualización de last_updated cuando id_sap/centro pasan de NULL a valor
    Lógica de negocio preservada intacta del original.
    """
    if df.empty:
        log.info("[clientes] DataFrame vacío. Nada que insertar.")
        return
    
    df = df.replace({np.nan: None, pd.NaT: None})
    df["id_sap"] = df["id_sap"].replace({np.nan: None, pd.NaT: None})

    # Obtener RUTs y NULLs actuales
    with engine.begin() as conn:
        ruts_db = {
            row[0]: row[1]
            for row in conn.execute(text(f"SELECT entity_id, usu_rut FROM {table_name}")).fetchall()
        }
        entity_ids_nulos = {
            row[0]
            for row in conn.execute(
                text(f"SELECT entity_id FROM {table_name} WHERE id_sap IS NULL OR centro IS NULL")
            ).fetchall()
        }
        if verbose:
            log.info(f"[clientes] {len(entity_ids_nulos)} registros con id_sap o centro NULL.")

    ruts_conflictivos = []

    with engine.begin() as conn:
        for _, row in df.iterrows():
            row_dict = _clean_records([row.to_dict()])[0]
            entity_id = row_dict["entity_id"]
            nuevo_rut_norm  = normalizar_rut(row_dict["usu_rut"])
            actual_rut_norm = normalizar_rut(ruts_db.get(entity_id))

            if ruts_db.get(entity_id) is not None and nuevo_rut_norm != actual_rut_norm:
                incluir_rut = False
                ruts_conflictivos.append({
                    "entity_id":   entity_id,
                    "rut_original": ruts_db.get(entity_id),
                    "rut_nuevo":    row_dict["usu_rut"],
                })
                if verbose:
                    log.warning(f"[clientes] FK conflict entity_id={entity_id}: {ruts_db.get(entity_id)} → {row_dict['usu_rut']}")
            else:
                incluir_rut = True

            columnas_update = [
                "id_sap = VALUES(id_sap)",
                "centro = VALUES(centro)",
                "razon_social = VALUES(razon_social)",
                "celular = VALUES(celular)",
            ]
            if incluir_rut:
                columnas_update.insert(0, "usu_rut = VALUES(usu_rut)")

            query = f"""
                INSERT INTO {table_name} (
                    entity_id, id_adobe, id_sap, usu_rut, centro, region, active,
                    razon_social, contacto, celular, email, created_at, last_conection, last_updated
                ) VALUES (
                    :entity_id, :id_adobe, :id_sap, :usu_rut, :centro, :region, :active,
                    :razon_social, :contacto, :celular, :email, :created_at, :last_conection, NULL
                )
                ON DUPLICATE KEY UPDATE {', '.join(columnas_update)};
            """
            try:
                conn.execute(text(query), row_dict)
            except Exception as e:
                log.error(f"[clientes] Error entity_id={entity_id}: {e}")

    if verbose and ruts_conflictivos:
        log.warning(f"[clientes] {len(ruts_conflictivos)} RUTs no actualizados por riesgo FK.")

    # Actualizar last_updated donde id_sap/centro pasaron de NULL a valor
    updated_df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    _actualizar_last_updated_clientes(updated_df, table_name, entity_ids_nulos, engine, verbose)
    log.info(f"[clientes] Upsert completado en {table_name}.")

def _actualizar_last_updated_clientes(df, table_name, entity_ids_nulos, engine, verbose=False):
    df = df.replace({np.nan: None, pd.NaT: None})
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    registros = df[
        df["entity_id"].isin(entity_ids_nulos)
        & df["id_sap"].notnull()
        & df["centro"].notnull()
    ]
    if registros.empty:
        return
    with engine.begin() as conn:
        for _, row in registros.iterrows():
            try:
                conn.execute(
                    text(f"UPDATE {table_name} SET last_updated = :ts WHERE entity_id = :eid"),
                    {"ts": today, "eid": row["entity_id"]},
                )
            except Exception as e:
                log.error(f"[clientes] Error last_updated entity_id={row['entity_id']}: {e}")
    if verbose:
        log.info(f"[clientes] last_updated actualizado en {len(registros)} registros.")

# ---------------------------------------------------------------------------
# SECCIÓN: Productos
# ---------------------------------------------------------------------------
def insertar_y_actualizar_productos(df_productos: pd.DataFrame, table_name: str, engine):
    """
    UPSERT bulk de productos por PK (sku).
    - gteq en extract garantiza overlap seguro
    - ON DUPLICATE KEY UPDATE absorbe duplicados sin error
    - Un solo roundtrip a DB independiente del volumen
    """
    df_productos = df_productos.where(pd.notnull(df_productos), None)
    if df_productos.empty:
        log.info("[productos] Sin productos para upsert.")
        return

    records = df_productos.to_dict("records")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {table_name} (
                    id, sku, name, created_at, updated_at,
                    weight_per_box, image_url, content_measurement_unit,
                    category, subcategory, brand, units_per_box,
                    average_weight_per_box, box_size, storage_description
                ) VALUES (
                    :id, :sku, :name, :created_at, :updated_at,
                    :weight_per_box, :image_url, :content_measurement_unit,
                    :category, :subcategory, :brand, :units_per_box,
                    :average_weight_per_box, :box_size, :storage_description
                )
                ON DUPLICATE KEY UPDATE
                    name                     = VALUES(name),
                    updated_at               = VALUES(updated_at),
                    weight_per_box           = VALUES(weight_per_box),
                    image_url                = VALUES(image_url),
                    content_measurement_unit = VALUES(content_measurement_unit),
                    category                 = VALUES(category),
                    subcategory              = VALUES(subcategory),
                    brand                    = VALUES(brand),
                    units_per_box            = VALUES(units_per_box),
                    average_weight_per_box   = VALUES(average_weight_per_box),
                    box_size                 = VALUES(box_size),
                    storage_description      = VALUES(storage_description)
            """),
            records,
        )

    log.info(f"[productos] {len(records):,} registros upserted en {table_name}.")

# ---------------------------------------------------------------------------
# SECCIÓN: Grid
# ---------------------------------------------------------------------------
def upsert_grid_with_status_tracking(df: pd.DataFrame, table_name: str, engine):
    """
    Upsert de grid con:
    - Validación de order_id contra ariztia_orders (FK)
    - Cancelación de SKUs que desaparecen del pedido
    - Bulk upsert por order_id + sku
    - Columnas actualizadas para reflejar campos de /rest/V1/orders items[]
    """
    df = df.where(pd.notnull(df), None)

    with engine.begin() as conn:
        valid_order_ids = {
            row[0] for row in conn.execute(text("SELECT order_id FROM ariztia_orders")).fetchall()
        }
        log.info(f"[grid] {len(valid_order_ids):,} order_ids válidos en ariztia_orders.")

        df = df[df["order_id"].isin(valid_order_ids)]
        if df.empty:
            log.info("[grid] Sin registros válidos tras filtrado por FK.")
            return

        for order_id, group in df.groupby("order_id"):
            # Cancelar SKUs que ya no vienen en el pedido
            db_skus = {
                row[0] for row in conn.execute(
                    text(f"SELECT sku FROM {table_name} WHERE order_id = :oid"),
                    {"oid": order_id},
                ).fetchall()
            }
            canceled = db_skus - set(group["sku"])
            if canceled:
                placeholders = ", ".join(f"'{s}'" for s in canceled)
                conn.execute(
                    text(f"UPDATE {table_name} SET status = 'cancelado' WHERE order_id = :oid AND sku IN ({placeholders})"),
                    {"oid": order_id},
                )

            # Columnas disponibles en el DataFrame
            cols_insert = [
                "order_id", "order_sap", "fecha_unificada", "fecha_compra",
                "hora_compra", "sku", "name", "product_id", "item_id", "marca",
                "precio", "cantidad", "venta_neta", "venta_bruta",
                "qty_canceled", "qty_invoiced", "qty_shipped", "status",
            ]
            # Solo usar columnas que existen en el df
            cols_insert = [c for c in cols_insert if c in group.columns]
            group_records = group[cols_insert].to_dict("records")

            cols_sql    = ", ".join(cols_insert)
            params_sql  = ", ".join(f":{c}" for c in cols_insert)
            update_sql  = ", ".join(
                f"{c} = VALUES({c})" for c in cols_insert
                if c not in ("order_id", "sku")
            )

            conn.execute(
                text(f"""
                    INSERT INTO {table_name} ({cols_sql})
                    VALUES ({params_sql})
                    ON DUPLICATE KEY UPDATE {update_sql}
                """),
                group_records,
            )

    log.info(f"[grid] Upsert completado en {table_name}.")

# ---------------------------------------------------------------------------
# SECCIÓN: Actualizar solo status de órdenes existentes
# ---------------------------------------------------------------------------
def actualizar_status_ordenes(df: pd.DataFrame, table_name: str, engine):
    """
    Actualiza SOLO el campo status de órdenes que ya existen en la DB.
    Se usa con órdenes filtradas por updated_at — órdenes viejas que
    cambiaron de status recientemente.
    No inserta órdenes nuevas.
    """
    if df.empty:
        return

    with engine.begin() as conn:
        existing_ids = {
            row[0] for row in conn.execute(
                text(f"SELECT order_id FROM {table_name}")
            ).fetchall()
        }

    actualizadas = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            order_id = row.get("entity_id")
            status   = row.get("status")
            if order_id in existing_ids and status:
                conn.execute(
                    text(f"""
                        UPDATE {table_name}
                        SET status = :status, last_updated_at = NOW()
                        WHERE order_id = :order_id AND status != :status
                    """),
                    {"status": status, "order_id": order_id},
                )
                actualizadas += 1

    log.info(f"[orders] Status actualizado en {actualizadas:,} órdenes existentes.")


def upsert_orders_with_status_tracking(df: pd.DataFrame, table_name: str, engine, verbose: bool = True):
    """
    Upsert de órdenes con:
    - Validación de RUT contra ariztia_clients (FK)
    - Detección y log de RUTs conflictivos
    - Status tracking + last_updated_at
    Lógica de negocio preservada del original.
    """
    df = df.where(pd.notnull(df), None)
    if df.empty:
        log.info("[orders] DataFrame vacío. Nada que insertar.")
        return

    with engine.begin() as conn:
        existing_orders = {
            row[0]: row[1]
            for row in conn.execute(text(f"SELECT order_id, status FROM {table_name}")).fetchall()
        }

    if verbose:
        log.info(f"[orders] {len(existing_orders):,} órdenes existentes en DB.")

    new_records      = df[~df["order_id"].isin(existing_orders)].copy()
    existing_records = df[ df["order_id"].isin(existing_orders)].copy()

    if verbose:
        log.info(f"[orders] Nuevas: {len(new_records):,} | Existentes: {len(existing_records):,}")

    # Insertar nuevas órdenes con INSERT IGNORE para tolerar duplicados de PK
    # (puede ocurrir al re-procesar rangos de fecha ya cargados)
    if not new_records.empty:
        records = _clean_records(new_records.to_dict("records"))
        cols     = list(new_records.columns)
        cols_sql = ", ".join(cols)
        vals_sql = ", ".join(f":{c}" for c in cols)
        with engine.begin() as conn:
            conn.execute(
                text(f"INSERT IGNORE INTO {table_name} ({cols_sql}) VALUES ({vals_sql})"),
                records,
            )
        log.info(f"[orders] {len(new_records):,} nuevas órdenes insertadas (INSERT IGNORE).")

    # Actualizar órdenes existentes
    registros_actualizados = set()
    campos = ["status", "total_final", "venta_neta", "venta_bruta",
              "fecha_despacho", "rut", "valor_envio", "monto_dscto"]

    with engine.begin() as conn:
        for _, row in existing_records.iterrows():
            order_id = row["order_id"]
            cambios, params = [], {"order_id": order_id}

            result = conn.execute(
                text(f"SELECT {', '.join(campos)} FROM {table_name} WHERE order_id = :order_id"),
                {"order_id": order_id},
            ).fetchone()

            if result:
                for i, campo in enumerate(campos):
                    if result[i] != row[campo]:
                        cambios.append(f"{campo} = :{campo}")
                        params[campo] = row[campo]

            if existing_orders.get(order_id) != row["status"]:
                registros_actualizados.add(order_id)

            if cambios:
                conn.execute(
                    text(f"UPDATE {table_name} SET {', '.join(cambios)} WHERE order_id = :order_id"),
                    params,
                )

        # Actualizar last_updated_at donde cambió el status
        if registros_actualizados:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for order_id in registros_actualizados:
                conn.execute(
                    text(f"UPDATE {table_name} SET last_updated_at = :now WHERE order_id = :oid"),
                    {"now": now, "oid": order_id},
                )
            log.info(f"[orders] last_updated_at actualizado en {len(registros_actualizados):,} órdenes.")

    log.info(f"[orders] Upsert completado en {table_name}.")

# ---------------------------------------------------------------------------
# SECCIÓN: Clientes Pendientes
# ---------------------------------------------------------------------------
def sync_clients_pending(df: pd.DataFrame, table_name: str, engine):
    """
    TRUNCATE + INSERT — clientes pendientes se reemplazan completamente
    en cada ejecución (comportamiento original preservado).
    """
    df = df.replace({np.nan: None, pd.NaT: None})
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        log.info(f"[pendientes] Tabla {table_name} truncada.")

    df.to_sql(name=table_name, con=engine, if_exists="append", index=False, chunksize=1000)
    log.info(f"[pendientes] {len(df):,} registros insertados en {table_name}.")

# ---------------------------------------------------------------------------
# SECCIÓN: Métricas
# ---------------------------------------------------------------------------
def actualizar_metricas(engine, metrics_table_name: str):
    """Inserta snapshot de métricas operacionales en cada corrida."""
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with engine.begin() as conn:
        try:
            total_clients  = conn.execute(text("SELECT COUNT(*) FROM ariztia_clients")).scalar()
            total_orders   = conn.execute(text("SELECT COUNT(*) FROM ariztia_orders")).scalar()
            total_products = conn.execute(text("SELECT COUNT(*) FROM ariztia_products")).scalar()
            conn.execute(
                text(f"""
                    INSERT INTO {metrics_table_name} (last_update, total_clients, total_orders, total_products)
                    VALUES (:ts, :clients, :orders, :products)
                """),
                {"ts": current_timestamp, "clients": total_clients,
                 "orders": total_orders, "products": total_products},
            )
            log.info(f"[metricas] clients={total_clients:,} | orders={total_orders:,} | products={total_products:,}")
        except Exception as e:
            log.error(f"[metricas] Error al insertar métricas: {e}")
