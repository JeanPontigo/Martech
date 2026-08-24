"""
data_base.py  — Pipeline PF B2B
=================================
Responsabilidad: definir y crear el esquema completo de la base de datos.

Estándar aplicado (patrón Ariztia):
- Recibe el engine como parámetro (no lo crea internamente)
- Incluye DDL de pipeline_state y pipeline_logs (delta loading + observabilidad)
- DDL de negocio preservado intacto del original
- print() reemplazados por logging estructurado
- Ejecutar con: python main.py --setup
"""

import logging

from sqlalchemy import text

log = logging.getLogger("pipeline.data_base")


def create_pf_b2b_database(engine):
    """
    Recrea todas las tablas del pipeline PF B2B.
    ADVERTENCIA: hace DROP de las tablas existentes — usar solo en setup inicial.
    """
    with engine.begin() as conn:
        log.info("Eliminando tablas existentes...")

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        for t in [
            "order_item", "orders", "product", "client", "company",
            "category", "pipeline_state", "pipeline_logs",
        ]:
            conn.execute(text(f"DROP TABLE IF EXISTS {t};"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        # ── PIPELINE STATE — delta loading ─────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE pipeline_state (
                entity     VARCHAR(64) PRIMARY KEY,
                last_sync  DATETIME    NOT NULL,
                updated_at DATETIME    DEFAULT CURRENT_TIMESTAMP
                           ON UPDATE CURRENT_TIMESTAMP
            );
        """))

        # ── PIPELINE LOGS — observabilidad para Looker Studio ──────────────────
        conn.execute(text("""
            CREATE TABLE pipeline_logs (
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
        """))

        # ── CATEGORY ───────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE category (
                category_id   INT PRIMARY KEY,
                parent_id     INT NULL,
                category_name VARCHAR(255),
                is_active     TINYINT(1),
                product_count INT,
                level         INT,
                path          TEXT,
                CONSTRAINT fk_parent_cat
                    FOREIGN KEY (parent_id)
                    REFERENCES category(category_id)
                    ON DELETE SET NULL
            );
        """))

        # ── COMPANY ────────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE company (
                company_id      INT PRIMARY KEY,
                company_name    VARCHAR(255),
                oficina_venta   VARCHAR(50),
                codigo_oficina  VARCHAR(50),
                company_code    VARCHAR(50),
                city            VARCHAR(100),
                region          VARCHAR(100),
                company_email   VARCHAR(255),
                status          VARCHAR(20),
                rut_company     VARCHAR(50),
                INDEX idx_oficina_venta (oficina_venta)
            );
        """))

        # ── CLIENT ─────────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE client (
                client_id           INT PRIMARY KEY,
                group_id            INT,
                group_name          VARCHAR(100),
                client_rut          VARCHAR(50),
                created_at          DATETIME,
                updated_at          DATETIME,
                fecha_ultima_compra DATETIME,
                client_email        VARCHAR(255),
                firstname           VARCHAR(100),
                lastname            VARCHAR(100),
                company_id          INT,
                company_rut         VARCHAR(50),
                ticket_promedio     INT,
                compras_totales     INT DEFAULT 0,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                INDEX idx_client_email (client_email),
                INDEX idx_client_rut   (client_rut)
            );
        """))

        # ── PRODUCT ────────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE product (
                sku               VARCHAR(255) PRIMARY KEY,
                sku_name          TEXT,
                product_type      VARCHAR(50),
                availability      INT,
                created_at        DATETIME,
                updated_at        DATETIME,
                peso_promedio_kg  DECIMAL(10,2),
                unidad_x_producto INT,
                marca             VARCHAR(255),
                category_id       INT,
                category          VARCHAR(255),
                sub_category      VARCHAR(255),
                image_sku         TEXT,
                FOREIGN KEY (category_id)
                    REFERENCES category(category_id)
                    ON DELETE SET NULL
            );
        """))

        # ── ORDERS ─────────────────────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE orders (
                order_id       INT PRIMARY KEY,
                client_id      INT,
                company_id     INT,
                created_at     DATETIME,
                updated_at     DATETIME,
                estado         VARCHAR(50) DEFAULT '0',
                total          DECIMAL(12,2),
                subtotal       DECIMAL(12,2),
                payment_method VARCHAR(100),
                descuento      DECIMAL(12,2),
                envio          DECIMAL(12,2),
                fecha_envio    DATETIME,
                ciudad         VARCHAR(100),
                region         VARCHAR(100),
                FOREIGN KEY (client_id)  REFERENCES client(client_id),
                FOREIGN KEY (company_id) REFERENCES company(company_id)
            );
        """))

        # ── ORDER_ITEM ─────────────────────────────────────────────────────────
        # Clave única lógica: (order_id, sku) — permite UPSERT sin AUTO_INCREMENT
        conn.execute(text("""
            CREATE TABLE order_item (
                order_item_id   INT AUTO_INCREMENT PRIMARY KEY,
                order_id        INT,
                sku_name        VARCHAR(255),
                sku             VARCHAR(255),
                sku_qty         INT,
                sku_value       DECIMAL(10,2),
                sku_total_value DECIMAL(12,2),
                base_price      DECIMAL(10,2),
                original_price  DECIMAL(10,2),
                en_oferta       BOOLEAN,
                UNIQUE KEY uq_order_sku (order_id, sku),
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (sku) REFERENCES product(sku)
            );
        """))

    log.info("Base de datos PF B2B creada / actualizada correctamente.")
