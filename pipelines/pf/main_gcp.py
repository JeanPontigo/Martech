"""
main_gcp.py — Pipeline GCP puro (ELT)
========================================
Responsabilidad: orquestar el pipeline ELT completo hacia BigQuery.

Diferencias respecto a main.py (legacy MySQL):
- Sin SQLAlchemy ni PyMySQL — 100% BigQuery
- Sin transform.py ni load.py — el pipeline solo hace EL (Extract + Load a Bronze)
- La T (Transform) la hace dbt Cloud sobre Silver y Gold
- pipeline_state y pipeline_logs en BigQuery (utils_gcp.py)
- _get_last_client_id y _get_last_company_id leen desde BigQuery Bronze
- Sin data_base.py ni --setup
- Sin actualizar_metricas_derivadas — las métricas las calcula dbt en Gold

Flujo:
  Magento API → extract.py (write_raw_to_bronze) → bronze.ecommerce
                                                  → bronze.pipeline_state
                                                  → bronze.pipeline_logs
                    ↓ (dbt Cloud — separado)
              silver.* → gold.* → API REST
"""

import argparse
import logging
import time
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from extract import (
    build_session,
    extraer_categorias,
    extraer_clientes_actualizados,
    extraer_clientes_nuevos,
    extraer_companias,
    extraer_companias_nuevas,
    extraer_company_access,
    extraer_company_access_full,
    extraer_ordenes,
    extraer_ordenes_actualizadas,
    extraer_productos,
    get_brand_mapping,
    get_customer_group_mapping,
)
from utils_gcp import (
    ci_art,
    get_last_sync,
    update_last_sync,
    write_log,
    DEFAULT_FALLBACK,
)

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
# Tenant
# ---------------------------------------------------------------------------
TENANT_ID = "pf"

# ---------------------------------------------------------------------------
# Mapa de entidades disponibles para --entity
# ---------------------------------------------------------------------------
ENTITIES = [
    "orders", "order_items", "clients",
    "products", "companies", "categories",
    "company_access", "all",
]

# ---------------------------------------------------------------------------
# Helpers internos — delta por ID desde BigQuery Bronze
# ---------------------------------------------------------------------------
def _get_last_client_id() -> int:
    """
    Lee el último client_id procesado desde pipeline_state en BigQuery.
    Más eficiente que escanear bronze.ecommerce — pipeline_state es una tabla mínima.
    Retorna 0 si no hay checkpoint previo (primera corrida → full load).
    """
    last = get_last_sync(TENANT_ID, "clients_id")
    try:
        return int(last) if last != DEFAULT_FALLBACK else 0
    except (ValueError, TypeError):
        return 0


def _get_last_company_id() -> int:
    """
    Lee el último company_id procesado desde pipeline_state en BigQuery.
    Más eficiente que escanear bronze.ecommerce — pipeline_state es una tabla mínima.
    Retorna 0 si no hay checkpoint previo (primera corrida → full load).
    """
    last = get_last_sync(TENANT_ID, "companies")
    try:
        return int(last) if last != DEFAULT_FALLBACK else 0
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# Pipeline por entidad — solo Extract + Load a Bronze
# La Transform ocurre en dbt Cloud (Silver y Gold)
# ---------------------------------------------------------------------------
def run_categories(session) -> pd.DataFrame:
    """
    Categorías — full load siempre (catálogo estable).
    write_raw_to_bronze se llama dentro de extraer_categorias.
    """
    log.info("[categories] Iniciando...")
    df_raw = extraer_categorias(session)
    # No tiene checkpoint de tiempo — siempre se recarga completo
    return df_raw


def run_companies(session) -> pd.DataFrame:
    """
    Compañías — delta por company_id desde pipeline_state en BigQuery.
    Primera corrida (last_id=0) → full load.
    Corridas siguientes → solo compañías con ID > último checkpoint.
    """
    log.info("[companies] Iniciando...")
    last_id = _get_last_company_id()

    if last_id == 0:
        log.info("[companies] Sin checkpoint previo — full load.")
        df_raw = extraer_companias(session)
    else:
        df_raw = extraer_companias_nuevas(session, start_id=last_id)

    if df_raw.empty:
        log.info("[companies] Sin compañías nuevas.")
        return pd.DataFrame()

    # Guardar último company_id en pipeline_state
    id_col = "company_id" if "company_id" in df_raw.columns else "id"
    if id_col in df_raw.columns and not df_raw.empty:
        ultimo_id = str(int(df_raw[id_col].max()))
        update_last_sync(TENANT_ID, "companies", ultimo_id, checkpoint_type="id")
        log.info(f"[companies] Checkpoint actualizado → company_id={ultimo_id}")

    return df_raw


def run_clients(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Clientes — delta por entity_id (nuevos) + updated_at (actualizados).
    write_raw_to_bronze se llama dentro de extraer_clientes*.
    """
    log.info("[clients] Iniciando...")
    last_id         = _get_last_client_id()
    df_nuevos       = extraer_clientes_nuevos(session, start_id=last_id + 1)
    df_actualizados = extraer_clientes_actualizados(session, dtFrom, dtTo)

    df_combined = pd.concat(
        [df_nuevos, df_actualizados], ignore_index=True
    ).drop_duplicates(subset="id", keep="last") \
        if not df_nuevos.empty or not df_actualizados.empty \
        else pd.DataFrame()

    if df_combined.empty:
        log.info("[clients] Sin clientes nuevos ni actualizados.")
        return pd.DataFrame()

    # Guardar último client_id en pipeline_state
    if not df_nuevos.empty and "id" in df_nuevos.columns:
        ultimo_id = str(int(df_nuevos["id"].max()))
        update_last_sync(TENANT_ID, "clients_id", ultimo_id, checkpoint_type="id")
        log.info(f"[clients] Checkpoint ID actualizado → client_id={ultimo_id}")

    if not df_nuevos.empty or not df_actualizados.empty:
        update_last_sync(TENANT_ID, "clients", dtTo, checkpoint_type="datetime")

    return df_combined


def run_products(session) -> pd.DataFrame:
    """
    Productos — delta por updated_at desde pipeline_state en BigQuery.
    write_raw_to_bronze se llama dentro de extraer_productos.
    """
    log.info("[products] Iniciando...")
    last_sync = get_last_sync(TENANT_ID, "products")
    df_raw    = extraer_productos(session, last_sync=last_sync)

    if df_raw.empty:
        log.info("[products] Sin productos nuevos desde el último sync.")
        return pd.DataFrame()

    update_last_sync(TENANT_ID, "products", df_raw["updated_at"].max(), checkpoint_type="datetime")
    return df_raw


def run_orders(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Órdenes — nuevas (created_at) + actualizadas (updated_at).
    write_raw_to_bronze se llama dentro de extraer_ordenes*.
    En GCP puro no hay transform ni load MySQL — Bronze es el destino.
    """
    log.info("[orders] Iniciando...")
    df_nuevas       = extraer_ordenes(session, dtFrom, dtTo)
    df_actualizadas = extraer_ordenes_actualizadas(session, dtFrom, dtTo)

    if not df_nuevas.empty:
        update_last_sync(TENANT_ID, "orders", dtTo, checkpoint_type="datetime")

    total = len(df_nuevas) + len(df_actualizadas)
    log.info(f"[orders] {len(df_nuevas):,} nuevas + {len(df_actualizadas):,} actualizadas → Bronze")
    return df_nuevas


def run_company_access(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Company access — correo de contacto real por sucursal, vía endpoint
    custom martech-extcompanyusers. Checkpoint por datetime (created_at),
    mismo patrón que orders/clients. Resuelve el dato que company_email
    (entity=companies) NUNCA tuvo: el contacto real de compra por sucursal,
    distinto de la cuenta técnica de registro.

    El campo 'password' se sanitiza dentro de extraer_company_access() —
    nunca llega a Bronze, ni siquiera crudo (hallazgo de seguridad ya
    levantado a TI de PF, Ley 21.719).
    """
    log.info("[company_access] Iniciando...")
    df_raw = extraer_company_access(session, dtFrom, dtTo)

    if df_raw.empty:
        log.info("[company_access] Sin accesos nuevos.")
        return pd.DataFrame()

    update_last_sync(TENANT_ID, "company_access", dtTo, checkpoint_type="datetime")
    return df_raw


def run_order_items(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    """
    Order items — extraídos desde el mismo endpoint de órdenes.
    write_bronze=False porque los items ya están en raw_json de orders.
    dbt Silver los expande con UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.items')).
    """
    log.info("[order_items] Iniciando...")
    df_raw = extraer_ordenes(session, dtFrom, dtTo, write_bronze=False)

    if df_raw.empty:
        log.info("[order_items] Sin órdenes nuevas — items ya en Bronze via orders.")
        return pd.DataFrame()

    if not df_raw.empty:
        update_last_sync(TENANT_ID, "order_items", dtTo, checkpoint_type="datetime")

    log.info(f"[order_items] {len(df_raw):,} órdenes — items disponibles en Bronze via entity=orders")
    return df_raw


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------
def run_pipeline(
    entity:    str  = "all",
    full_load: bool = False,
    from_date: str  = None,
    to_date:   str  = None,
):
    """
    Ejecuta el pipeline ELT completo o por entidad.
    - full_load=True  → resetea checkpoints en pipeline_state BigQuery
    - full_load=False → delta loading desde pipeline_state BigQuery
    - from_date/to_date → override manual del rango
    """
    session        = build_session()
    now            = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    pipeline_start = time.time()

    log.info("=" * 60)
    log.info(f"PIPELINE PF GCP INICIADO — entity={entity} | full_load={full_load} | tenant={TENANT_ID}")
    log.info("=" * 60)

    def resolve_dates(entity_key):
        dt_from = from_date or get_last_sync(TENANT_ID, entity_key)
        dt_to   = to_date   or now
        return dt_from, dt_to

    if full_load:
        log.info("[pipeline] full_load=True — ignorando pipeline_state, usando DEFAULT_FALLBACK")

    # Mapa de tareas
    task_map = {
        "categories":      lambda: run_categories(session),
        "companies":       lambda: run_companies(session),
        "clients":         lambda: run_clients(session, *resolve_dates("clients")),
        "products":        lambda: run_products(session),
        "orders":          lambda: run_orders(session, *resolve_dates("orders")),
        "order_items":     lambda: run_order_items(session, *resolve_dates("order_items")),
        "company_access":  lambda: run_company_access(session, *resolve_dates("company_access")),
    }

    tasks   = list(task_map.items()) if entity == "all" else [(entity, task_map[entity])]
    results = {}

    for name, fn in tasks:
        t0 = time.time()
        dt_from, dt_to = resolve_dates(name) \
            if name not in ("categories", "companies", "products") \
            else (None, None)
        try:
            df      = fn()
            elapsed = time.time() - t0
            status  = "empty" if df is None or df.empty else "success"

            write_log(
                tenant_id=TENANT_ID, entity=name, status=status,
                records_extracted=len(df) if df is not None else 0,
                duration_sec=elapsed,
                date_from=dt_from, date_to=dt_to,
            )
            count = len(df) if df is not None else 0
            log.info(f"  ✓ {name:<22} {count:>6,} registros   ({elapsed:.1f}s)")
            results[name] = df

        except Exception as e:
            elapsed = time.time() - t0
            write_log(
                tenant_id=TENANT_ID, entity=name, status="error",
                records_extracted=0, duration_sec=elapsed,
                date_from=dt_from, date_to=dt_to, error_msg=str(e),
            )
            log.error(f"  ✗ {name:<22} ERROR: {e}   ({elapsed:.1f}s)")
            results[name] = pd.DataFrame()

    total = time.time() - pipeline_start
    log.info("=" * 60)
    log.info(f"PIPELINE PF GCP COMPLETADO en {total:.1f}s")
    log.info("=" * 60)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline ELT PF → BigQuery Bronze (GCP puro)",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--entity",
        default="all",
        choices=ENTITIES,
        help=(
            "Entidad a procesar:\n"
            "  all          → todas (default)\n"
            "  orders       → órdenes nuevas + actualizadas\n"
            "  order_items  → items (desde Bronze via orders)\n"
            "  clients      → clientes nuevos + actualizados\n"
            "  products     → catálogo de productos\n"
            "  companies    → empresas B2B\n"
            "  categories   → árbol de categorías (full load)\n"
            "  company_access → contacto real por sucursal (correo, vía\n"
            "                    endpoint custom martech-extcompanyusers)\n"
        ),
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        default=False,
        help="Ignora pipeline_state y extrae desde DEFAULT_FALLBACK (2020-01-01)",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Fuerza dtFrom manualmente — ignora pipeline_state",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Fuerza dtTo manualmente (default: ahora UTC)",
    )
    return parser


def main():
    ci_art()
    parser = build_parser()
    args   = parser.parse_args()

    run_pipeline(
        entity=args.entity,
        full_load=args.full_load,
        from_date=args.from_date,
        to_date=args.to_date,
    )

    # Publicar mensaje en Pub/Sub para disparar dbt Cloud
    try:
        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("martech-data-platform-atlas", "pipeline-pf")
        future = publisher.publish(topic_path, b"pipeline-pf completed")
        future.result()
        print("[pubsub] Mensaje publicado en pipeline-pf")
    except Exception as e:
        print(f"[pubsub] Error al publicar mensaje: {e}")


if __name__ == "__main__":
    main()
