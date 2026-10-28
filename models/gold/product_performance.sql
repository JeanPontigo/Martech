-- models/gold/product_performance.sql
-- Fuente: silver.fact_order_items + silver.fact_orders + silver.dim_product
-- Granularidad: una fila por SKU por dia por tenant
-- Metricas de rendimiento de productos

-- total_units_sold cuenta todas las unidades movidas, incluidas las que
-- vienen dentro de un pack. En Magento el padre del pack (product_type =
-- 'bundle') lleva el dinero y como cantidad el numero de packs; los hijos
-- llevan las unidades reales con monto cero. Por eso:
--   - avg_unit_price divide por unidades_facturadas, no por total_units_sold
--   - sumar total_units_sold ENTRE SKUs cuenta doble (packs + unidades):
--     para eso, excluir product_type = 'bundle'
-- Solo aplica a mc. pf no tiene packs; el custom de aatn no entrega
-- product_type, asi que ahi llega NULL y no se puede distinguir.
{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}
WITH
-- -----------------------------------------------------------------------
-- 1. Leer Silver
-- -----------------------------------------------------------------------
orders AS (
    SELECT
        order_id,
        tenant_id,
        client_id,
        DATE(created_at_chile) AS fecha
    FROM {{ ref('fact_orders') }}
    WHERE status_norm = 'pagada'
),
items AS (
    SELECT
        order_id,
        tenant_id,
        sku,
        product_type,
        qty_ordered,
        line_total_net,
        discount_amount
    FROM {{ ref('fact_order_items') }}
),
products AS (
    SELECT
        tenant_id,
        sku,
        sku_name,
        brand,
        category_name,
        subcategory_name
    FROM {{ ref('dim_product') }}
),
-- -----------------------------------------------------------------------
-- 2. JOIN items con ordenes
-- -----------------------------------------------------------------------
joined AS (
    SELECT
        o.tenant_id,
        o.fecha,
        o.order_id,
        o.client_id,
        i.sku,
        i.product_type,
        i.qty_ordered,
        i.line_total_net,
        i.discount_amount
    FROM orders o
    JOIN items i ON o.order_id = i.order_id AND o.tenant_id = i.tenant_id
),
-- -----------------------------------------------------------------------
-- 3. Agregar por SKU por dia
-- -----------------------------------------------------------------------
aggregated AS (
    SELECT
        tenant_id,
        fecha,
        sku,
        MAX(product_type)                                           AS product_type,
        COUNT(DISTINCT order_id)                                    AS total_orders,
        COUNT(DISTINCT client_id)                                   AS unique_customers,
        SUM(qty_ordered)                                            AS total_units_sold,
        SUM(IF(line_total_net > 0, qty_ordered, 0))                 AS unidades_facturadas,
        SUM(line_total_net)                                         AS total_revenue,
        ROUND(SAFE_DIVIDE(
            SUM(line_total_net),
            SUM(IF(line_total_net > 0, qty_ordered, 0))
        ), 2)                                                       AS avg_unit_price,
        SUM(discount_amount)                                        AS total_discount
    FROM joined
    GROUP BY tenant_id, fecha, sku
),
-- -----------------------------------------------------------------------
-- 4. JOIN con dim_product
-- -----------------------------------------------------------------------
with_product AS (
    SELECT
        a.tenant_id,
        a.fecha,
        a.sku,
        a.product_type,
        p.sku_name,
        p.brand,
        p.category_name     AS category,
        p.subcategory_name  AS sub_category,
        a.total_orders,
        a.unique_customers,
        a.total_units_sold,
        a.unidades_facturadas,
        a.total_revenue,
        a.avg_unit_price,
        a.total_discount
    FROM aggregated a
    LEFT JOIN products p
        ON  a.sku       = p.sku
        AND a.tenant_id = p.tenant_id
)
-- -----------------------------------------------------------------------
-- 5. Output final
-- -----------------------------------------------------------------------
SELECT
    TO_HEX(SHA256(CONCAT(
        tenant_id,
        CAST(fecha AS STRING),
        sku
    )))                                                             AS kpi_id,
    tenant_id,
    fecha,
    sku,
    product_type,
    sku_name,
    brand,
    category,
    sub_category,
    total_orders,
    unique_customers,
    total_units_sold,
    unidades_facturadas,
    total_revenue,
    avg_unit_price,
    total_discount,
    CURRENT_DATETIME('UTC')                                         AS last_updated
FROM with_product

