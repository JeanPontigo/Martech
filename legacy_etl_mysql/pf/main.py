"""
main.py  — Pipeline PF B2B
============================
Responsabilidad: orquestar el pipeline ETL completo.

Este módulo NO contiene lógica de negocio — solo coordina
extract → transform → load por cada entidad.

Estándar aplicado (patrón Ariztia):
- CLI extendido: --entity, --full-load, --from-date, --to-date, --setup
- Delta loading real via pipeline_state en DB (no fechas hardcodeadas)
- pipeline_logs escrito después de cada entidad (observabilidad)
- HTTP Session única compartida entre todas las extracciones
- Mappings auxiliares (marcas, grupos, categorías) cargados UNA sola vez
  y propagados como parámetros (sin side effects en transform)
- Logging estructurado con duración por entidad
- Errores por entidad aislados (una falla no detiene el pipeline)
- CSV intermedios eliminados — carga directa a MySQL
"""

import argparse
import logging
import time
from datetime import datetime

import pandas as pd

from extract import (
    build_session,
    extraer_categorias,
    extraer_clientes_actualizados,
    extraer_clientes_nuevos,
    extraer_companias,
    extraer_companias_nuevas,
    extraer_ordenes,
    extraer_ordenes_actualizadas,
    extraer_productos,
    get_brand_mapping,
    get_customer_group_mapping,
)
from transform import (
    transform_categorias,
    transform_clientes,
    transform_companias,
    transform_order_items,
    transform_orders,
    transform_productos,
)
from load import (
    actualizar_estado_ordenes,
    actualizar_metricas_derivadas,
    cargar_categorias,
    get_last_sync,
    reset_sync,
    update_last_sync,
    upsert_clientes,
    upsert_companias,
    upsert_order_items,
    upsert_orders,
    upsert_productos,
    write_log,
)
from utils import ci_art, create_db_conn
from data_base import create_pf_b2b_database

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
ENTITIES = [
    "orders", "order_items", "clients",
    "products", "companies", "categories", "all",
]

# ---------------------------------------------------------------------------
# Pipeline por entidad
# ---------------------------------------------------------------------------
def run_categories(session, engine) -> pd.DataFrame:
    log.info("[categories] Iniciando...")
    df_raw = extraer_categorias(session)
    df_t   = transform_categorias(df_raw)
    cargar_categorias(df_t, engine)
    # No tiene checkpoint de tiempo — siempre se recarga completo
    return df_t


def run_companies(session, engine) -> pd.DataFrame:
    log.info("[companies] Iniciando...")

    last_id = _get_last_company_id(engine)

    if last_id == 0:
        # Primera carga — full load
        log.info("[companies] Tabla vacía — ejecutando full load.")
        df_raw = extraer_companias(session)
    else:
        # Delta por ID — solo compañías nuevas desde el último company_id
        df_raw = extraer_companias_nuevas(session, start_id=last_id)

    if df_raw.empty:
        log.info("[companies] Sin compañías nuevas.")
        return pd.DataFrame()

    df_t = transform_companias(df_raw)
    upsert_companias(df_t, engine)
    return df_t


def run_clients(session, engine, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[clients] Iniciando...")

    last_id = _get_last_client_id(engine)

    if last_id == 0:
        # Tabla vacía (full load): extrae todos los clientes por ID desde 0.
        # NO usar extraer_clientes_actualizados con rango histórico desde 2020
        # — genera ~1.500 páginas en paralelo y mata el servidor con timeouts.
        log.info("[clients] Tabla vacía — full load por entity_id desde 0.")
        df_nuevos      = extraer_clientes_nuevos(session, start_id=0)
        df_actualizados = pd.DataFrame()
    else:
        # Corrida normal: delta por ID para nuevos + updated_at para actualizados
        df_nuevos       = extraer_clientes_nuevos(session, start_id=last_id + 1)
        df_actualizados = extraer_clientes_actualizados(session, dtFrom, dtTo)

    # Mapping de grupos (se pasa como parámetro, sin side effects en transform)
    group_map = get_customer_group_mapping(session)

    df_combined = (
        pd.concat([df_nuevos, df_actualizados], ignore_index=True)
          .drop_duplicates(subset="id", keep="last")
        if not df_nuevos.empty or not df_actualizados.empty
        else pd.DataFrame()
    )

    if df_combined.empty:
        log.info("[clients] Sin clientes nuevos ni actualizados.")
        return pd.DataFrame()

    df_t = transform_clientes(df_combined, group_map)
    upsert_clientes(df_t, engine)

    if not df_nuevos.empty:
        update_last_sync(engine, "clients", dtTo)

    return df_t


def run_products(session, engine) -> pd.DataFrame:
    log.info("[products] Iniciando...")
    last_sync = get_last_sync(engine, "products")
    df_raw    = extraer_productos(session, last_sync=last_sync)

    if df_raw.empty:
        log.info("[products] Sin productos nuevos desde el último sync.")
        return pd.DataFrame()

    # Mappings auxiliares pasados como parámetros
    brand_map    = get_brand_mapping(session)
    category_map = _build_category_map(engine)

    df_t = transform_productos(df_raw, brand_map, category_map)
    upsert_productos(df_t, engine)
    update_last_sync(engine, "products", df_raw["updated_at"].max())
    return df_t


def run_orders(session, engine, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[orders] Iniciando...")

    # Órdenes NUEVAS (created_at) → INSERT
    df_nuevas = extraer_ordenes(session, dtFrom, dtTo)
    # Órdenes MODIFICADAS (updated_at) → UPDATE estado
    df_actualizadas = extraer_ordenes_actualizadas(session, dtFrom, dtTo)

    if not df_nuevas.empty:
        df_orders_t = transform_orders(df_nuevas)
        upsert_orders(df_orders_t, engine)
        update_last_sync(engine, "orders", dtTo)

    if not df_actualizadas.empty:
        df_act_t = transform_orders(df_actualizadas)
        actualizar_estado_ordenes(df_act_t, engine)

    return df_nuevas


def run_order_items(session, engine, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[order_items] Iniciando...")
    df_raw = extraer_ordenes(session, dtFrom, dtTo)

    if df_raw.empty:
        log.info("[order_items] Sin órdenes nuevas para expandir items.")
        return pd.DataFrame()

    # Columnas necesarias para items
    item_cols = [
        "order_id", "sku", "sku_name",
        "precio_unitario", "cantidad", "total_linea",
        "base_price", "original_price",
    ]
    item_cols_presentes = [c for c in item_cols if c in df_raw.columns]
    df_items_t = transform_order_items(df_raw[item_cols_presentes])
    upsert_order_items(df_items_t, engine)

    if not df_raw.empty:
        update_last_sync(engine, "order_items", dtTo)

    return df_items_t

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------
def _get_last_client_id(engine) -> int:
    """Devuelve el MAX(client_id) actual en DB, o 0 si la tabla está vacía."""
    try:
        result = pd.read_sql("SELECT MAX(client_id) AS ultimo_id FROM client;", engine)
        val = result.iloc[0]["ultimo_id"]
        return int(val) if val is not None else 0
    except Exception as e:
        log.warning(f"[clients] No se pudo obtener último client_id: {e}")
        return 0


def _get_last_company_id(engine) -> int:
    """Devuelve el MAX(company_id) actual en DB, o 0 si la tabla está vacía."""
    try:
        result = pd.read_sql("SELECT MAX(company_id) AS ultimo_id FROM company;", engine)
        val = result.iloc[0]["ultimo_id"]
        return int(val) if val is not None else 0
    except Exception as e:
        log.warning(f"[companies] No se pudo obtener último company_id: {e}")
        return 0


def _build_category_map(engine) -> dict:
    """
    Construye {category_id_str: category_name} desde la tabla category en DB.
    Se llama después de cargar categorías, así siempre está actualizado.
    """
    try:
        df = pd.read_sql("SELECT category_id, category_name FROM category;", engine)
        return {str(row.category_id): row.category_name for row in df.itertuples()}
    except Exception as e:
        log.warning(f"[products] No se pudo leer category map desde DB: {e}")
        return {}

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
    - from_date/to_date → override manual del rango
    """
    if full_load:
        reset_sync(engine, entity if entity != "all" else None)

    session         = build_session()
    now             = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pipeline_start  = time.time()

    log.info("=" * 60)
    log.info(f"PIPELINE PF INICIADO — entity={entity} | full_load={full_load}")
    log.info("=" * 60)

    def resolve_dates(entity_key):
        dt_from = from_date or get_last_sync(engine, entity_key)
        dt_to   = to_date   or now
        return dt_from, dt_to

    # Mapa de tareas
    # Categorías y compañías siempre se recargan completas (sin rango de fechas)
    task_map = {
        "categories":  lambda: run_categories(session, engine),
        "companies":   lambda: run_companies(session, engine),
        "clients":     lambda: run_clients(session, engine, *resolve_dates("clients")),
        "products":    lambda: run_products(session, engine),
        "orders":      lambda: run_orders(session, engine, *resolve_dates("orders")),
        "order_items": lambda: run_order_items(session, engine, *resolve_dates("order_items")),
    }

    # Orden de ejecución respeta dependencias FK:
    # categories → companies → clients → products → orders → order_items
    EXECUTION_ORDER = [
        "categories", "companies", "clients",
        "products", "orders", "order_items",
    ]

    tasks = (
        [(name, task_map[name]) for name in EXECUTION_ORDER]
        if entity == "all"
        else [(entity, task_map[entity])]
    )

    results = {}

    for name, fn in tasks:
        t0 = time.time()
        dt_from, dt_to = resolve_dates(name) if name not in (
            "categories", "companies", "products"
        ) else (None, None)

        try:
            df      = fn()
            elapsed = time.time() - t0
            count   = len(df) if df is not None and not df.empty else 0
            status  = "empty" if count == 0 else "success"

            write_log(
                engine=engine, entity=name, status=status,
                records_extracted=count, duration_sec=elapsed,
                dt_from=dt_from, dt_to=dt_to,
            )
            log.info(f"  ✓ {name:<18} {count:>6,} registros   ({elapsed:.1f}s)")
            results[name] = df

        except Exception as e:
            elapsed = time.time() - t0
            write_log(
                engine=engine, entity=name, status="error",
                records_extracted=0, duration_sec=elapsed,
                dt_from=dt_from, dt_to=dt_to, error_msg=str(e),
            )
            log.error(f"  ✗ {name:<18} ERROR: {e}   ({elapsed:.1f}s)")
            results[name] = pd.DataFrame()

    # Métricas derivadas solo en corrida completa
    if entity == "all":
        try:
            actualizar_metricas_derivadas(engine)
        except Exception as e:
            log.error(f"[metricas] {e}")

    total = time.time() - pipeline_start
    log.info("=" * 60)
    log.info(f"PIPELINE PF COMPLETADO en {total:.1f}s")
    log.info("=" * 60)

    return results

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL PF B2B",
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
            "  all          → todas (default, respeta orden FK)\n"
            "  categories   → árbol de categorías\n"
            "  companies    → compañías B2B\n"
            "  clients      → clientes (delta por ID + updated_at)\n"
            "  products     → catálogo de productos (delta por updated_at)\n"
            "  orders       → órdenes (nuevas + actualización de estado)\n"
            "  order_items  → líneas de orden\n"
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
            create_pf_b2b_database(engine)
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
