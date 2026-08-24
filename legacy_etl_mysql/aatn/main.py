"""
main.py
=======
Responsabilidad: orquestar el pipeline ETL completo.

Cambios respecto al original:
- run_orders: INSERT desde custom, UPDATE status desde nativo
- run_grid:   consume el mismo DataFrame del custom (sin segunda extracción)
- Eliminado:  extraer_grid, enriquecer_ordenes
- Sin cambios: run_products, run_clients_approved, run_clients_pending, CLI
"""

import argparse
import logging
import time
from datetime import datetime

import pandas as pd

from extract import (
    build_session,
    extraer_clientes_aprobados,
    extraer_clientes_pendientes,
    extraer_ordenes_actualizadas,
    extraer_ordenes_custom,
    extraer_productos,
)
from transform import (
    process_order_data,
    transform_clientes,
    transform_clientes_pendientes,
    transform_grid,
    transform_orders,
    transform_productos,
)
from load import (
    actualizar_status_ordenes,
    cargar_info_orders,
    get_last_sync,
    insertar_o_actualizar_clientes,
    insertar_y_actualizar_productos,
    reset_sync,
    sync_clients_pending,
    update_last_sync,
    upsert_grid_with_status_tracking,
    upsert_orders_with_status_tracking,
    write_log,
)
from utils import ci_art, create_db_conn
from data_base import run_setup_ariztia

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Mapa de entidades disponibles para --entity
# ---------------------------------------------------------------------------
ENTITIES = ["orders", "clients_approved", "products", "grid", "clients_pending", "all"]

# ---------------------------------------------------------------------------
# Pipeline por entidad
# ---------------------------------------------------------------------------
def run_orders(session, engine, dtFrom, dtTo):
    """
    Paso 1 — INSERT de órdenes nuevas desde el endpoint custom.
    Paso 2 — UPDATE de status desde el endpoint nativo (updated_at).
    La conversión UTC-4 → UTC ocurre dentro de extraer_ordenes_actualizadas().
    """
    log.info("[orders] Iniciando...")

    # Paso 1: órdenes nuevas desde custom → INSERT
    df_raw = extraer_ordenes_custom(session, dtFrom, dtTo)

    if not df_raw.empty:
        df_orders_t    = transform_orders(df_raw)
        df_info_orders = process_order_data(df_orders_t)
        upsert_orders_with_status_tracking(df_orders_t, "ariztia_orders", engine)
        cargar_info_orders(df_info_orders, "info_orders", engine)
        update_last_sync(engine, "orders", dtTo)

    # Paso 2: órdenes modificadas desde nativo → UPDATE status únicamente
    df_actualizadas = extraer_ordenes_actualizadas(session, dtFrom, dtTo)
    if not df_actualizadas.empty:
        actualizar_status_ordenes(df_actualizadas, "ariztia_orders", engine)

    return df_raw


def run_grid(session, engine, dtFrom, dtTo):
    """
    Grid extraído del mismo DataFrame del custom — sin segunda extracción.
    Las filas del custom ya son items individuales; transform_grid las mapea
    directamente al schema de ariztia_grid.
    """
    log.info("[grid] Iniciando...")

    df_raw = extraer_ordenes_custom(session, dtFrom, dtTo)

    if df_raw.empty:
        log.info("[grid] Sin datos del custom para cargar.")
        return pd.DataFrame()

    df_t = transform_grid(df_raw)
    upsert_grid_with_status_tracking(df_t, "ariztia_grid", engine)

    if not df_t.empty:
        update_last_sync(engine, "grid", dtTo)

    return df_t


def run_products(session, engine):
    log.info("[products] Iniciando...")
    last_sync = get_last_sync(engine, "products")
    df_raw    = extraer_productos(session, last_sync=last_sync)
    df_t      = transform_productos(df_raw)
    insertar_y_actualizar_productos(df_t, "ariztia_products", engine)

    if not df_raw.empty:
        update_last_sync(engine, "products", df_raw["updated_at"].max())

    return df_t


def run_clients_approved(session, engine, dtFrom, dtTo):
    log.info("[clients_approved] Iniciando...")
    df_raw = extraer_clientes_aprobados(session, dtFrom, dtTo)
    df_t   = transform_clientes(df_raw)
    insertar_o_actualizar_clientes(df_t, "ariztia_clients", engine)

    if not df_raw.empty:
        update_last_sync(engine, "clients_approved", dtTo)

    return df_t


def run_clients_pending(session, engine):
    log.info("[clients_pending] Iniciando...")
    df_raw = extraer_clientes_pendientes(session)
    df_t   = transform_clientes_pendientes(df_raw)
    sync_clients_pending(df_t, "ariztia_clients_pending", engine)
    return df_t

# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------
def run_pipeline(engine, entity: str = "all", full_load: bool = False,
                 from_date: str = None, to_date: str = None):
    """
    Ejecuta el pipeline ETL completo o por entidad.
    - full_load=True  → resetea checkpoints (carga histórica)
    - full_load=False → delta loading desde pipeline_state
    - from_date/to_date → override manual del rango
    """
    if full_load:
        reset_sync(engine, entity if entity != "all" else None)

    session        = build_session()
    # dtTo por defecto: inicio del día actual (medianoche) — garantiza días de negocio completos
    # y evita el gap de madrugada que generaba usar NOW() como checkpoint.
    dt_to_default  = datetime.combine(datetime.now().date(), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
    pipeline_start = time.time()

    log.info("=" * 60)
    log.info(f"PIPELINE INICIADO — entity={entity} | full_load={full_load}")
    log.info("=" * 60)

    def resolve_dates(entity_key):
        dt_from = from_date or get_last_sync(engine, entity_key)
        dt_to   = to_date   or dt_to_default
        return dt_from, dt_to

    task_map = {
        "orders":           lambda: run_orders(
                                session, engine, *resolve_dates("orders")),
        "clients_approved": lambda: run_clients_approved(
                                session, engine, *resolve_dates("clients_approved")),
        "products":         lambda: run_products(session, engine),
        "grid":             lambda: run_grid(
                                session, engine, *resolve_dates("grid")),
        "clients_pending":  lambda: run_clients_pending(session, engine),
    }

    tasks   = list(task_map.items()) if entity == "all" else [(entity, task_map[entity])]
    results = {}

    for name, fn in tasks:
        t0 = time.time()
        dt_from, dt_to = resolve_dates(name) if name not in ("products", "clients_pending") \
                         else (None, None)
        try:
            df      = fn()
            elapsed = time.time() - t0
            status  = "empty" if df.empty else "success"

            write_log(
                engine=engine, entity=name, status=status,
                records_extracted=len(df), duration_sec=elapsed,
                dt_from=dt_from, dt_to=dt_to,
            )
            log.info(f"  ✓ {name:<22} {len(df):>6,} registros   ({elapsed:.1f}s)")
            results[name] = df

        except Exception as e:
            elapsed = time.time() - t0
            write_log(
                engine=engine, entity=name, status="error",
                records_extracted=0, duration_sec=elapsed,
                dt_from=dt_from, dt_to=dt_to, error_msg=str(e),
            )
            log.error(f"  ✗ {name:<22} ERROR: {e}   ({elapsed:.1f}s)")
            results[name] = pd.DataFrame()

    total = time.time() - pipeline_start
    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETADO en {total:.1f}s")
    log.info("=" * 60)

    return results

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Ariztía",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-s", "--setup",
        action="store_true",
        help="Construir la base de datos desde cero (ejecutar solo la primera vez)",
    )
    parser.add_argument(
        "--entity",
        default="all",
        choices=ENTITIES,
        help=(
            "Entidad a procesar:\n"
            "  all               → todas (default)\n"
            "  orders            → órdenes + info_orders\n"
            "  grid              → grid de órdenes\n"
            "  clients_approved  → clientes aprobados\n"
            "  clients_pending   → clientes pendientes\n"
            "  products          → catálogo de productos\n"
        ),
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        default=False,
        help="Resetea el checkpoint y extrae desde 2020-01-01 (carga histórica completa)",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Fuerza dtFrom manualmente — ignora el checkpoint",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Fuerza dtTo manualmente (default: ahora)",
    )
    return parser


def main():
    ci_art()
    parser = build_parser()
    args   = parser.parse_args()
    engine = create_db_conn()

    if args.setup:
        log.info("Modo setup: construyendo base de datos desde cero...")
        try:
            run_setup_ariztia()
            log.info("Setup completado.")
        except Exception as e:
            log.error(f"Error en setup: {e}")
        return

    run_pipeline(
        engine=engine,
        entity=args.entity,
        full_load=args.full_load,
        from_date=args.from_date,
        to_date=args.to_date,
    )


if __name__ == "__main__":
    main()
