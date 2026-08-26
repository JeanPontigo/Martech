"""
main_gcp_carozzi.py — Pipeline ELT Carozzi → BigQuery
======================================================
Responsabilidad: orquestar el pipeline ELT completo de Carozzi hacia BigQuery.

Flujo ELT:
  API Magento Carozzi (mercadocarozzi.cl)
    → extract_gcp_carozzi.py (write_raw_to_bronze)
    → bronze.ecommerce
    → bronze.pipeline_state
    → bronze.pipeline_logs
         ↓ (dbt Cloud — separado)
    silver.* → gold.*
"""

import argparse
import logging
import time
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

from extract_gcp_carozzi import (
    build_session,
    extraer_ordenes,
    extraer_ordenes_actualizadas,
    extraer_clientes_nuevos,
    extraer_clientes_actualizados,
    extraer_productos,
    extraer_categorias,
)
from utils_gcp_carozzi import (
    ci_czz,
    get_last_sync,
    update_last_sync,
    write_log,
    ping_bq,
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
TENANT_ID = "carozzi"

# ---------------------------------------------------------------------------
# Entidades disponibles para --entity
# ---------------------------------------------------------------------------
ENTITIES = [
    "orders", "clients", "products", "categories", "all",
]

# ---------------------------------------------------------------------------
# Pipeline por entidad
# ---------------------------------------------------------------------------
def run_orders(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[orders] Iniciando...")
    df_raw = extraer_ordenes(session, dtFrom, dtTo)
    if not df_raw.empty:
        update_last_sync(TENANT_ID, "orders", dtTo, checkpoint_type="datetime")
    df_actualizadas = extraer_ordenes_actualizadas(session, dtFrom, dtTo)
    log.info(f"[orders] {len(df_raw):,} nuevas | {len(df_actualizadas):,} actualizadas")
    return df_raw


def run_clients(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[clients] Iniciando...")
    
    try:
        last_client_id = int(get_last_sync(TENANT_ID, "clients_id") or 0)
    except ValueError:
        last_client_id = 0

    df_nuevos = extraer_clientes_nuevos(session, start_id=last_client_id)
    if not df_nuevos.empty:
        nuevo_id = int(df_nuevos["id"].max()) if "id" in df_nuevos.columns else last_client_id
        update_last_sync(TENANT_ID, "clients_id", str(nuevo_id), checkpoint_type="id")

    df_actualizados = extraer_clientes_actualizados(session, dtFrom, dtTo)
    if not df_actualizados.empty:
        update_last_sync(TENANT_ID, "clients", dtTo, checkpoint_type="datetime")

    log.info(f"[clients] {len(df_nuevos):,} nuevos | {len(df_actualizados):,} actualizados")
    return df_nuevos


def run_products(session) -> pd.DataFrame:
    log.info("[products] Iniciando...")
    last_sync = get_last_sync(TENANT_ID, "products")
    df_raw    = extraer_productos(session, last_sync=last_sync)
    if not df_raw.empty:
        nuevo_sync = df_raw["updated_at"].max() if "updated_at" in df_raw.columns else None
        if nuevo_sync:
            update_last_sync(TENANT_ID, "products", str(nuevo_sync), checkpoint_type="datetime")
    return df_raw


def run_categories(session) -> pd.DataFrame:
    log.info("[categories] Iniciando...")
    df_raw = extraer_categorias(session)
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
    load_dotenv()

    session        = build_session()
    now            = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    pipeline_start = time.time()

    if not ping_bq():
        log.warning("[pipeline] BigQuery no disponible — abortando")
        return {}

    log.info("=" * 60)
    log.info(
        f"PIPELINE CAROZZI GCP INICIADO — "
        f"entity={entity} | full_load={full_load} | tenant={TENANT_ID}"
    )
    log.info("=" * 60)

    def resolve_dates(entity_key: str) -> tuple[str, str]:
        dt_from = from_date or get_last_sync(TENANT_ID, entity_key)
        dt_to   = to_date   or now
        return dt_from, dt_to

    task_map = {
        "orders":     lambda: run_orders(session, *resolve_dates("orders")),
        "clients":    lambda: run_clients(session, *resolve_dates("clients")),
        "products":   lambda: run_products(session),
        "categories": lambda: run_categories(session),
    }

    tasks   = list(task_map.items()) if entity == "all" else [(entity, task_map[entity])]
    results = {}

    for name, fn in tasks:
        t0 = time.time()
        dt_from, dt_to = resolve_dates(name) \
            if name not in ("products", "categories") \
            else (None, None)
        try:
            df      = fn()
            elapsed = time.time() - t0
            status  = "empty" if (df is None or df.empty) else "success"
            count   = len(df) if df is not None else 0

            write_log(
                tenant_id=TENANT_ID, entity=name, status=status,
                records_extracted=count, duration_sec=elapsed,
                date_from=dt_from, date_to=dt_to,
            )
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
    log.info(f"PIPELINE CAROZZI GCP COMPLETADO en {total:.1f}s")
    log.info("=" * 60)

    # Publicar mensaje en Pub/Sub para disparar dbt Cloud
    try:
        from google.cloud import pubsub_v1
        publisher  = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("martech-data-platform-atlas", "pipeline-carozzi")
        future     = publisher.publish(topic_path, b"pipeline-carozzi completed")
        future.result()
        print("[pubsub] Mensaje publicado en pipeline-carozzi")
    except Exception as e:
        print(f"[pubsub] Error al publicar mensaje: {e}")

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline ELT Carozzi → BigQuery Bronze",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--entity",
        default="all",
        choices=ENTITIES,
        help=(
            "Entidad a procesar:\n"
            "  all         → todas (default)\n"
            "  orders      → órdenes nuevas + actualizadas\n"
            "  clients     → clientes\n"
            "  products    → catálogo de productos\n"
            "  categories  → árbol de categorías\n"
        ),
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        default=False,
        help="Extrae desde 2020-01-01 (carga histórica completa)",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Fuerza dtFrom manualmente",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD HH:MM:SS",
        help="Fuerza dtTo manualmente",
    )
    return parser


def main():
    ci_czz()
    parser = build_parser()
    args   = parser.parse_args()

    run_pipeline(
        entity=args.entity,
        full_load=args.full_load,
        from_date=args.from_date,
        to_date=args.to_date,
    )


if __name__ == "__main__":
    main()
