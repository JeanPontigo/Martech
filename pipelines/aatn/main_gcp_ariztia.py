"""
main_gcp_ariztia.py — Pipeline ELT Ariztia → BigQuery
====================================================
Responsabilidad: orquestar el pipeline ELT completo de Ariztia hacia BigQuery.

Flujo ELT:
  API Magento/Custom Ariztia
    → extract_gcp_ariztia.py (write_raw_to_bronze antes del aplanamiento)
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

from extract_gcp_ariztia import (
    build_session,
    extraer_ordenes_custom,
    extraer_ordenes_actualizadas,
    extraer_clientes_aprobados,
    extraer_clientes_pendientes,
    extraer_productos,
    extraer_companias,
)
from utils_gcp_ariztia import (
    ci_art,
    get_last_sync,
    update_last_sync,
    write_log,
    ping_bq,
    get_bq_client,
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
TENANT_ID = "aatn"

# ---------------------------------------------------------------------------
# Entidades disponibles para --entity
# ---------------------------------------------------------------------------
ENTITIES = [
    "orders", "clients_approved",
    "clients_pending", "products", "companies", "all",
]

# ---------------------------------------------------------------------------
# Pipeline por entidad
# ---------------------------------------------------------------------------
def run_orders(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[orders] Iniciando...")

    df_raw = extraer_ordenes_custom(session, dtFrom, dtTo)

    if not df_raw.empty:
        update_last_sync(TENANT_ID, "orders", dtTo, checkpoint_type="datetime")

    df_actualizadas = extraer_ordenes_actualizadas(session, dtFrom, dtTo)

    log.info(f"[orders] {len(df_raw):,} filas custom | {len(df_actualizadas):,} actualizadas")
    return df_raw


def run_clients_approved(session, dtFrom: str, dtTo: str) -> pd.DataFrame:
    log.info("[clients_approved] Iniciando...")

    df_raw = extraer_clientes_aprobados(session, dtFrom, dtTo)

    if not df_raw.empty:
        update_last_sync(TENANT_ID, "clients_approved", dtTo, checkpoint_type="datetime")

    return df_raw


def run_clients_pending(session) -> pd.DataFrame:
    log.info("[clients_pending] Iniciando...")
    df_raw = extraer_clientes_pendientes(session)
    return df_raw


def run_products(session, full_load: bool = False) -> pd.DataFrame:
    log.info("[products] Iniciando...")

    # BUGFIX (2026-08): full_load nunca se conectaba con la lógica real —
    # el flag se logueaba pero get_last_sync siempre aplicaba el checkpoint
    # existente, incluso con --full-load. Ahora, si full_load=True, se
    # ignora el checkpoint por completo (last_sync=None → sin filtro de
    # fecha en extraer_productos → trae el catálogo completo).
    last_sync = None if full_load else get_last_sync(TENANT_ID, "products")
    df_raw    = extraer_productos(session, last_sync=last_sync)

    if not df_raw.empty:
        nuevo_sync = df_raw["updated_at"].max() if "updated_at" in df_raw.columns else None
        if nuevo_sync:
            update_last_sync(TENANT_ID, "products", str(nuevo_sync), checkpoint_type="datetime")

    return df_raw


def run_companies(full_load: bool = False) -> pd.DataFrame:
    """
    Compañías — checkpoint por ID (no por fecha). El endpoint no expone
    updated_at de status, así que solo se capturan compañías NUEVAS
    (id > último id ingestado). Un cambio de status en una compañía ya
    existente no se detecta hasta que exista delta real por fecha
    (pendiente — solicitado a TI).
    """
    log.info("[companies] Iniciando...")

    last_id_raw = "0" if full_load else get_last_sync(TENANT_ID, "companies")
    try:
        last_id = int(last_id_raw) if last_id_raw and last_id_raw != DEFAULT_FALLBACK else 0
    except (ValueError, TypeError):
        last_id = 0

    df_raw = extraer_companias(last_id=last_id)

    if not df_raw.empty and "id" in df_raw.columns:
        nuevo_id = int(df_raw["id"].max())
        update_last_sync(TENANT_ID, "companies", str(nuevo_id), checkpoint_type="id")
        log.info(f"[companies] Checkpoint actualizado → id={nuevo_id}")

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
    bq_client      = get_bq_client()
    now            = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    pipeline_start = time.time()

    if not ping_bq():
        log.warning("[pipeline] BigQuery no disponible — abortando")
        return {}

    log.info("=" * 60)
    log.info(
        f"PIPELINE ARIZTIA GCP INICIADO — "
        f"entity={entity} | full_load={full_load} | tenant={TENANT_ID}"
    )
    log.info("=" * 60)

    def resolve_dates(entity_key: str) -> tuple[str, str]:
        # BUGFIX (2026-08): full_load no se conectaba con la resolución de
        # fechas — solo --from-date explícito funcionaba realmente. Ahora,
        # con full_load=True y sin --from-date explícito, se usa
        # DEFAULT_FALLBACK en lugar del checkpoint guardado.
        if from_date:
            dt_from = from_date
        elif full_load:
            dt_from = DEFAULT_FALLBACK
        else:
            dt_from = get_last_sync(TENANT_ID, entity_key)
        dt_to = to_date or now
        return dt_from, dt_to

    task_map = {
        "orders":           lambda: run_orders(session, *resolve_dates("orders")),
        "clients_approved": lambda: run_clients_approved(session, *resolve_dates("clients_approved")),
        "clients_pending":  lambda: run_clients_pending(session),
        "products":         lambda: run_products(session, full_load=full_load),
        "companies":        lambda: run_companies(full_load=full_load),
    }

    tasks   = list(task_map.items()) if entity == "all" else [(entity, task_map[entity])]
    results = {}

    for name, fn in tasks:
        t0 = time.time()
        dt_from, dt_to = resolve_dates(name) \
            if name not in ("clients_pending", "products", "companies") \
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
    log.info(f"PIPELINE ARIZTIA GCP COMPLETADO en {total:.1f}s")
    log.info("=" * 60)

    # Publicar mensaje en Pub/Sub para disparar dbt Cloud
    try:
        from google.cloud import pubsub_v1
        publisher  = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("martech-data-platform-atlas", "pipeline-ariztia")
        future     = publisher.publish(topic_path, b"pipeline-ariztia completed")
        future.result()
        print("[pubsub] Mensaje publicado en pipeline-ariztia")
    except Exception as e:
        print(f"[pubsub] Error al publicar mensaje: {e}")

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline ELT Ariztia → BigQuery Bronze",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--entity",
        default="all",
        choices=ENTITIES,
        help=(
            "Entidad a procesar:\n"
            "  all               → todas (default)\n"
            "  orders            → órdenes nuevas (custom) + actualizadas (nativo)\n"
            "  grid              → grid de órdenes\n"
            "  clients_approved  → clientes aprobados\n"
            "  clients_pending   → clientes pendientes (full load)\n"
            "  products          → catálogo de productos\n"
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
    ci_art()
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
