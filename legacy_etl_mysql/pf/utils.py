"""
utils.py  — Pipeline PF B2B
=============================
Funciones compartidas por todos los módulos del pipeline.

Estándar aplicado (patrón Ariztia):
- Token y URL leídos desde .env (nunca hardcodeados)
- SSH tunnel encapsulado aquí (no en cada script)
- Logging estructurado
"""

import logging
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

log = logging.getLogger("pipeline.utils")

# ---------------------------------------------------------------------------
# Conexión a base de datos
# ---------------------------------------------------------------------------
def create_db_conn():
    """
    Crea y devuelve un engine SQLAlchemy.
    Lee las credenciales desde .env — nunca hardcodeadas.
    """
    load_dotenv(override=True)

    user     = os.getenv("MYSQL_USER", "").strip()
    password = os.getenv("MYSQL_PASSWORD", "").strip()
    host     = os.getenv("MYSQL_HOST", "127.0.0.1").strip()
    port     = os.getenv("MYSQL_PORT", "3306").strip()
    dbname   = os.getenv("MYSQL_DBNAME", "").strip()

    connection_url = (
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
    )
    engine = create_engine(connection_url, pool_recycle=3600)
    log.info(f"Conectando a {host}:{port}/{dbname}")
    return engine

# ---------------------------------------------------------------------------
# URL y headers de la API Magento
# ---------------------------------------------------------------------------
def build_url(endpoint: str) -> str:
    """Construye la URL base + endpoint. Trailing slash se normaliza."""
    load_dotenv(override=True)
    base = os.getenv("PF_URL", "https://tiendapfalimentos.cl").rstrip("/")
    endpoint = endpoint.lstrip("/")
    return f"{base}/{endpoint}"


def build_headers() -> dict:
    """Construye los headers de autenticación leyendo el token desde .env."""
    load_dotenv(override=True)
    token = os.getenv("PF_API_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

# ---------------------------------------------------------------------------
# Normalización de RUT
# ---------------------------------------------------------------------------
def normalizar_rut(rut) -> str | None:
    """Formato #####-x — sin puntos, con guión, k minúscula."""
    if pd.isnull(rut):
        return None
    v = str(rut).replace(".", "").lower().strip()
    if not v:
        return None
    if "-" not in v and len(v) > 1:
        v = f"{v[:-1]}-{v[-1]}"
    return v

# ---------------------------------------------------------------------------
# Banner de inicio
# ---------------------------------------------------------------------------
def ci_art():
    art = r"""
    =====================================================================================================
    ____  ____   ____  ____  ____
    |  _ \|  __| |  _ \|_  / |  _ \
    | |_) | |_   | |_) |/ /  | |_) |
    |  __/|  _|  |  _ </ /__ |  __/
    |_|   |_|    |_| \_\____||_|
                    ETL — Adobe Commerce — Magento 2.4.6 — PF B2B
    =====================================================================================================
    """
    print(art)
