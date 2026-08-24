-- models/gold/category_performance.sql
-- Fuente: silver.fact_order_items + silver.fact_orders + silver.dim_product
-- Granularidad: una fila por categoria + subcategoria + fecha
-- Tenant: todos
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
        created_at
    FROM {{ ref('fact_orders') }}
),
items AS (
    SELECT
        order_id,
        tenant_id,
        sku,
        line_total_net,
        qty_ordered
    FROM {{ ref('fact_order_items') }}
),
products AS (
    SELECT
        tenant_id,
        sku,
        category_name,
        subcategory_name
    FROM {{ ref('dim_product') }}
),
-- -----------------------------------------------------------------------
-- 2. JOIN items con ordenes y productos
-- -----------------------------------------------------------------------
joined AS (
    SELECT
        p.category_name                     AS category,
        p.subcategory_name                  AS sub_category,
        o.created_at                        AS fecha,
        oi.line_total_net,
        oi.qty_ordered,
        oi.order_id,
        oi.tenant_id
    FROM items oi
    JOIN orders o
        ON oi.order_id = o.order_id AND oi.tenant_id = o.tenant_id
    LEFT JOIN products p
        ON oi.sku = p.sku AND oi.tenant_id = p.tenant_id
)
-- -----------------------------------------------------------------------
-- 3. Output final
-- -----------------------------------------------------------------------
SELECT
    tenant_id,
    category,
    sub_category,
    fecha,
    SUM(line_total_net)         AS venta_total,
    SUM(qty_ordered)            AS cantidad_producto,
    COUNT(DISTINCT order_id)    AS presencia_orders
FROM joined
GROUP BY tenant_id, category, sub_category, fecha
