"""
bbdd.py
=======
Responsabilidad: DDL de la base de datos devmartech_carozzi.

Cambios respecto al original:
- Eliminado bloque CREATE TABLE metricas (reemplazada por pipeline_logs)
- Agregado run_setup_pipeline() → crea pipeline_state y pipeline_logs
- CREATE TABLE usa IF NOT EXISTS en todos los casos (idempotente)
- Eliminada dependencia de tqdm (logging estructurado)
- time.sleep eliminado
"""

import logging
from sqlalchemy.sql import text
from sqlalchemy import inspect
from connect import crear_conexion

log = logging.getLogger("pipeline.bbdd")


# ---------------------------------------------------------------------------
# Tablas de negocio (sin cambios de schema respecto al original)
# ---------------------------------------------------------------------------
def crear_bd():
    """
    Crea las tablas de negocio si no existen.
    Idempotente — seguro de ejecutar múltiples veces.
    """
    engine = crear_conexion()

    with engine.begin() as conn:
        log.info("Revisando tablas de negocio...")
        existing = set(inspect(conn).get_table_names())

        if "category" not in existing:
            conn.execute(text("""
                CREATE TABLE category (
                    category_id   INT PRIMARY KEY,
                    parent_id     INT,
                    category_name VARCHAR(250),
                    is_active     TINYINT(1),
                    product_count INT
                )
            """))
            log.info("Tabla 'category' creada.")
        else:
            log.info("Tabla 'category' ya existe.")

        if "product" not in existing:
            conn.execute(text("""
                CREATE TABLE product (
                    entity_id              INT PRIMARY KEY,
                    sku                    VARCHAR(255) NOT NULL,
                    sku_name               TEXT,
                    price                  BIGINT,
                    status                 INT,
                    visibility             INT,
                    type_id                TEXT,
                    created_at             DATETIME,
                    updated_at             DATETIME,
                    linked_product_type    TEXT,
                    product_image          TEXT,
                    peso_promedio_kg       DECIMAL(10,6),
                    marca                  VARCHAR(255),
                    category_id            INT,
                    category_name          VARCHAR(255),
                    sub_category           VARCHAR(255),
                    position               INT,
                    FOREIGN KEY (category_id) REFERENCES category(category_id)
                        ON DELETE SET NULL
                )
            """))
            log.info("Tabla 'product' creada.")
        else:
            log.info("Tabla 'product' ya existe.")

        if "customers" not in existing:
            conn.execute(text("""
                CREATE TABLE customers (
                    customer_id           INT PRIMARY KEY,
                    customer_group_id     INT,
                    customer_group_name   TEXT,
                    created_at            DATETIME,
                    updated_at            DATETIME,
                    customer_email        TEXT,
                    customer_firstname    VARCHAR(255),
                    customer_lastname     VARCHAR(255),
                    company               VARCHAR(255),
                    rut                   TEXT,
                    region                VARCHAR(255),
                    city                  VARCHAR(255),
                    last_order_date       DATETIME,
                    total_orders          INT,
                    is_first_order        TINYINT(1)
                )
            """))
            log.info("Tabla 'customers' creada.")
        else:
            log.info("Tabla 'customers' ya existe.")

        if "orders" not in existing:
            conn.execute(text("""
                CREATE TABLE orders (
                    order_id            INT PRIMARY KEY,
                    base_grand_total    DECIMAL(10,2),
                    created_at          DATETIME,
                    updated_at          DATETIME,
                    grand_total         DECIMAL(10,2),
                    shipping_amount     DECIMAL(10,2),
                    customer_id         INT,
                    customer_email      TEXT,
                    state               VARCHAR(50),
                    status              VARCHAR(50),
                    subtotal            DECIMAL(10,2),
                    discount_amount     DECIMAL(10,2),
                    order_city          VARCHAR(100),
                    order_region        VARCHAR(100),
                    peso_promedio_kg    DECIMAL(10,6),
                    method              TEXT,
                    total_item_count    INT,
                    total_qty_ordered   INT,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
            """))
            log.info("Tabla 'orders' creada.")
        else:
            log.info("Tabla 'orders' ya existe.")

        if "orders_items" not in existing:
            conn.execute(text("""
                CREATE TABLE orders_items (
                    item_id         INT PRIMARY KEY,
                    order_id        INT,
                    created_at      DATETIME,
                    updated_at      DATETIME,
                    product_id      INT NULL,
                    product_type    TEXT,
                    original_price  DECIMAL(10,2),
                    qty_ordered     INT,
                    qty_invoiced    INT,
                    qty_shipped     INT,
                    sku             VARCHAR(255) NOT NULL,
                    CONSTRAINT fk_orders_items_product
                        FOREIGN KEY (product_id) REFERENCES product(entity_id)
                            ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id)
                )
            """))
            log.info("Tabla 'orders_items' creada.")
        else:
            log.info("Tabla 'orders_items' ya existe.")

    log.info("Setup de tablas de negocio completado.")


# ---------------------------------------------------------------------------
# Tablas de infraestructura del pipeline
# ---------------------------------------------------------------------------
def run_setup_pipeline(engine):
    """
    Crea las tablas de infraestructura del pipeline:
    - pipeline_state → checkpoint de delta loading por entidad
    - pipeline_logs  → observabilidad de cada ejecución

    Reemplaza la tabla 'metricas' que fue eliminada.
    Idempotente — seguro de ejecutar múltiples veces.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_state (
                entity     VARCHAR(64) PRIMARY KEY,
                last_sync  DATETIME    NOT NULL,
                updated_at DATETIME    DEFAULT CURRENT_TIMESTAMP
                           ON UPDATE CURRENT_TIMESTAMP
            )
        """))
        log.info("Tabla 'pipeline_state' lista.")

        conn.execute(text("""
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
            )
        """))
        log.info("Tabla 'pipeline_logs' lista.")

    log.info("Setup de tablas de pipeline completado.")
