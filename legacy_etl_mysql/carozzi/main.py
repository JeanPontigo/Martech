"""
main.py
=======
Responsabilidad: orquestar el pipeline ETL completo de Carozzi.

Este módulo NO contiene lógica de negocio — solo coordina
extract → transform → load por cada entidad.

Cambios respecto al original:
- CLI con argparse: --entity, --full-load, --from-date, --to-date, --setup
- Delta loading real via pipeline_state en DB (reemplaza max(updated_at) en Excel)
- pipeline_logs escrito después de cada entidad (observabilidad)
- HTTP Session única compartida entre todas las extracciones
- Errores por entidad aislados (una falla no detiene el pipeline)
- Eliminadas: crear_excel, tabla metricas, validacion.xlsx, tqdm
- Logging estructurado con duración por entidad
"""

import argparse
import logging
import time
from datetime import datetime

import pandas as pd

from extract import (
    build_session,
    extraer_category,
    extraer_productos,
    extraer_customers,
    extraer_orders,
    extraer_orders_nuevas,
    extraer_orders_actualizadas,
    extraer_orders_items,
    extraer_orders_items_nuevos,
    extraer_orders_items_actualizados,
    extraer_marcas,
    extraer_customers_group,
)
from transform import (
    transform_category,
    transform_productos,
    transform_customers,
    transform_orders,
    transform_orders_items,
)
from load import (
    get_last_sync,
    update_last_sync,
    reset_sync,
    write_log,
    load_category,
    load_productos,
    load_customers,
    load_orders,
    actualizar_status_ordenes,
    load_orders_items,
    actualizar_datos_customers,
)
from utils import create_db_conn
from bbdd import crear_bd, run_setup_pipeline
from registro import setup_logging

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
# Entidades disponibles para --entity
# ---------------------------------------------------------------------------
ENTITIES = ["category", "products", "customers", "orders", "order_items", "all"]


# ---------------------------------------------------------------------------
# Pipeline por entidad
# ---------------------------------------------------------------------------
def run_category(session, engine) -> pd.DataFrame:
    log.info("[category] Iniciando...")
    df_raw = extraer_category(session)
    df_t   = transform_category(df_raw, for_load=True)
    load_category(df_t, engine)
    return df_t


def run_products(session, engine) -> pd.DataFrame:
    log.info("[products] Iniciando...")
    last_sync = get_last_sync(engine, "products")
    df_raw    = extraer_productos(session, last_sync=last_sync)

    if df_raw.empty:
        return pd.DataFrame()

    # for_load=False preserva level/path/tipo necesarios para el enriquecimiento
    df_marcas   = extraer_marcas(session)
    df_cat_raw  = extraer_category(session)
    df_cat      = transform_category(df_cat_raw, for_load=False)

    df_t = transform_productos(df_raw, df_cat, df_marcas)
    load_productos(df_t, engine)
    update_last_sync(engine, "products", str(df_raw["updated_at"].max()))
    return df_t


def run_customers(session, engine, to_date: str = None) -> pd.DataFrame:
    log.info("[customers] Iniciando...")
    now       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt_to     = to_date or now
    last_sync = get_last_sync(engine, "customers")
    df_raw    = extraer_customers(session, last_sync=last_sync)

    if df_raw.empty:
        return pd.DataFrame()

    df_groups = extraer_customers_group(session)
    df_t      = transform_customers(df_raw, df_groups)
    load_customers(df_t, engine)
    update_last_sync(engine, "customers", dt_to)
    return df_t


def run_orders(session, engine, from_date: str = None, to_date: str = None) -> pd.DataFrame:
    log.info("[orders] Iniciando...")
    now     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt_from = from_date or get_last_sync(engine, "orders")
    dt_to   = to_date or now

    # Paso 1: órdenes NUEVAS (created_at) → INSERT
    df_nuevas = extraer_orders_nuevas(session, dt_from, dt_to)

    # Paso 2: órdenes MODIFICADAS (updated_at) → UPDATE status
    df_actualizadas = extraer_orders_actualizadas(session, dt_from, dt_to)

    if not df_nuevas.empty:
        df_t = transform_orders(df_nuevas)
        load_orders(df_t, engine)
        update_last_sync(engine, "orders", dt_to)

    if not df_actualizadas.empty:
        actualizar_status_ordenes(df_actualizadas, engine)

    return df_nuevas if not df_nuevas.empty else pd.DataFrame()


def run_order_items(session, engine, from_date: str = None, to_date: str = None) -> pd.DataFrame:
    log.info("[order_items] Iniciando...")
    now     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt_from = from_date or get_last_sync(engine, "order_items")
    dt_to   = to_date or now

    # Paso 1: items NUEVOS (created_at) → INSERT
    df_nuevos = extraer_orders_items_nuevos(session, dt_from, dt_to)

    # Paso 2: items MODIFICADOS (updated_at) → UPDATE cantidades y estados
    df_actualizados = extraer_orders_items_actualizados(session, dt_from, dt_to)

    if not df_nuevos.empty:
        df_t = transform_orders_items(df_nuevos)
        load_orders_items(df_t, engine)
        update_last_sync(engine, "order_items", dt_to)

    if not df_actualizados.empty:
        df_t = transform_orders_items(df_actualizados)
        load_orders_items(df_t, engine)

    return df_nuevos if not df_nuevos.empty else pd.DataFrame()


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------
def run_pipeline(
    engine,
    entity: str = "all",
    full_load: bool = False,
    from_date: str = None,
    to_date: str = None,
):
    """
    Ejecuta el pipeline ETL completo o por entidad.
    - full_load=True  → resetea checkpoints (carga histórica)
    - full_load=False → delta loading desde pipeline_state
    - from_date/to_date → override manual del rango (útil para recuperación)
    """
    if full_load:
        reset_sync(engine, entity if entity != "all" else None)

    setup_logging()
    session        = build_session()
    now            = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pipeline_start = time.time()

    log.info("=" * 60)
    log.info("PIPELINE INICIADO — entity=%s | full_load=%s", entity, full_load)
    log.info("=" * 60)

    task_map = {
        "category":    lambda: run_category(session, engine),
        "products":    lambda: run_products(session, engine),
        "customers":   lambda: run_customers(session, engine, to_date=to_date or now),
        "orders":      lambda: run_orders(session, engine, from_date=from_date, to_date=to_date or now),
        "order_items": lambda: run_order_items(session, engine, from_date=from_date, to_date=to_date or now),
    }

    tasks  = list(task_map.items()) if entity == "all" else [(entity, task_map[entity])]
    results = {}

    for name, fn in tasks:
        t0 = time.time()
        # Resolver dt_from/dt_to para el log (referencial)
        dt_from = from_date or get_last_sync(engine, name)
        dt_to   = to_date or now

        try:
            df      = fn()
            elapsed = time.time() - t0
            status  = "empty" if df.empty else "success"

            write_log(
                engine=engine, entity=name, status=status,
                records_extracted=len(df), duration_sec=elapsed,
                dt_from=dt_from, dt_to=dt_to,
            )
            log.info("  ✓ %-18s %8s registros   (%.1fs)", name, f"{len(df):,}", elapsed)
            results[name] = df

        except Exception as e:
            elapsed = time.time() - t0
            write_log(
                engine=engine, entity=name, status="error",
                records_extracted=0, duration_sec=elapsed,
                dt_from=dt_from, dt_to=dt_to, error_msg=str(e),
            )
            log.error("  ✗ %-18s ERROR: %s   (%.1fs)", name, e, elapsed)
            results[name] = pd.DataFrame()

    # Post-load de columnas derivadas en customers — solo en corrida completa
    if entity in ("all", "customers", "orders"):
        try:
            actualizar_datos_customers(engine)
        except Exception as e:
            log.error("[customers post-load] %s", e)

    total = time.time() - pipeline_start
    log.info("=" * 60)
    log.info("PIPELINE COMPLETADO en %.1fs", total)
    log.info("=" * 60)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Carozzi",
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
            "  all          → todas (default)\n"
            "  category     → árbol de categorías\n"
            "  products     → catálogo de productos\n"
            "  customers    → clientes\n"
            "  orders       → órdenes\n"
            "  order_items  → items de órdenes\n"
        ),
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        default=False,
        help="Resetea el checkpoint y extrae desde el inicio (carga histórica completa)",
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
    parser = build_parser()
    args   = parser.parse_args()
    engine = create_db_conn()

    if args.setup:
        log.info("Modo setup: construyendo base de datos...")
        try:
            crear_bd()
            run_setup_pipeline(engine)
            log.info("Setup completado.")
        except Exception as e:
            log.error("Error en setup: %s", e)
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
