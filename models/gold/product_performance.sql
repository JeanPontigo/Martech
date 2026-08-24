-- models/gold/product_performance.sql
-- Fuente: silver.fact_order_items + silver.fact_orders + silver.dim_product
-- Granularidad: una fila por SKU por dia por tenant
-- Metricas de rendimiento de productos
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
        DATE(created_at) AS fecha
    FROM {{ ref('fact_orders') }}
),
items AS (
    SELECT
        order_id,
        tenant_id,
        sku,
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
        COUNT(DISTINCT order_id)                                    AS total_orders,
        COUNT(DISTINCT client_id)                                   AS unique_customers,
        SUM(qty_ordered)                                            AS total_units_sold,
        SUM(line_total_net)                                         AS total_revenue,
        ROUND(SAFE_DIVIDE(SUM(line_total_net), SUM(qty_ordered)), 2) AS avg_unit_price,
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
        p.sku_name,
        p.brand,
        p.category_name     AS category,
        p.subcategory_name  AS sub_category,
        a.total_orders,
        a.unique_customers,
        a.total_units_sold,
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
    sku_name,
    brand,
    category,
    sub_category,
    total_orders,
    unique_customers,
    total_units_sold,
    total_revenue,
    avg_unit_price,
    total_discount,
    CURRENT_DATETIME('UTC')                                         AS last_updated
FROM with_product

