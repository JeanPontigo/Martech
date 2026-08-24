"""
utils.py
========
Responsabilidad: helpers compartidos entre los módulos del pipeline.

- build_session()   → HTTP Session con connection pooling + retry/backoff
- build_headers()   → headers de autenticación Magento
- create_db_conn()  → engine SQLAlchemy desde variables de entorno
"""

import os
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("pipeline.utils")

MAX_WORKERS = 8


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def build_headers() -> dict:
    return {
        "Authorization": f"Bearer {os.getenv('MC_TOKEN')}",
        "Content-Type": "application/json",
    }


def build_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """
    Session reutilizable con:
    - Pool de conexiones TCP (evita handshake por cada request)
    - Retry automático en 429/500/502/503/504
      delay = backoff * (2 ^ intento) → 1s, 2s, 4s ...
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS * 2,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(build_headers())
    return session


def build_url(path: str) -> str:
    base = os.getenv("MC_URL", "").rstrip("/")
    return f"{base}{path}"


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------
def create_db_conn():
    """Engine SQLAlchemy desde variables de entorno."""
    url = URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        database=os.getenv("MYSQL_DBNAME_MCETL"),
    )
    engine = create_engine(url, pool_recycle=3600)
    log.info("Conexión con la base de datos establecida.")
    return engine
