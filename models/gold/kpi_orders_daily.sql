-- models/gold/kpi_orders_daily.sql
-- Fuente: silver.fact_orders + silver.fact_order_items + silver.dim_product
-- Granularidad: una fila por dia + tenant + canal
-- Incluye top 5 SKUs y categorias del dia en formato JSON

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
        DATE(created_at)    AS fecha,
        canal_compra        AS canal,
        status,
        subtotal_net
    FROM {{ ref('fact_orders') }}
),

items AS (
    SELECT
        oi.order_id,
        oi.tenant_id,
        oi.sku,
        oi.qty_ordered,
        oi.line_total_net
    FROM {{ ref('fact_order_items') }} oi
),

-- -----------------------------------------------------------------------
-- 2. Metricas base por dia + tenant + canal
-- -----------------------------------------------------------------------
order_metrics AS (
    SELECT
        tenant_id,
        fecha,
        canal,
        COUNT(DISTINCT order_id)                                                AS total_orders,
        SUM(subtotal_net)                                                       AS total_revenue,
        ROUND(SAFE_DIVIDE(SUM(subtotal_net), COUNT(DISTINCT order_id)), 2)      AS avg_ticket,
        COUNTIF(status = 'completado')                                          AS orders_completed,
        COUNTIF(status = 'cancelado')                                           AS orders_canceled
    FROM orders
    GROUP BY tenant_id, fecha, canal
),

-- -----------------------------------------------------------------------
-- 3. Total items por dia + tenant + canal
-- -----------------------------------------------------------------------
item_metrics AS (
    SELECT
        o.tenant_id,
        o.fecha,
        o.canal,
        SUM(i.qty_ordered)                                                      AS total_items
    FROM orders o
    LEFT JOIN items i ON o.order_id = i.order_id AND o.tenant_id = i.tenant_id
    GROUP BY o.tenant_id, o.fecha, o.canal
),

-- -----------------------------------------------------------------------
-- 4. Top 5 SKUs por dia + tenant + canal
-- -----------------------------------------------------------------------
sku_ranked AS (
    SELECT
        o.tenant_id,
        o.fecha,
        o.canal,
        i.sku,
        SUM(i.line_total_net)                                                   AS sku_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY o.tenant_id, o.fecha, o.canal
            ORDER BY SUM(i.line_total_net) DESC
        )                                                                       AS sku_rank
    FROM orders o
    LEFT JOIN items i ON o.order_id = i.order_id AND o.tenant_id = i.tenant_id
    WHERE i.sku IS NOT NULL
    GROUP BY o.tenant_id, o.fecha, o.canal, i.sku
),

top_skus AS (
    SELECT
        tenant_id,
        fecha,
        canal,
        TO_JSON_STRING(
            ARRAY_AGG(
                STRUCT(sku, sku_revenue)
                ORDER BY sku_rank
            )
        )                                                                       AS top_skus
    FROM sku_ranked
    WHERE sku_rank <= 5
    GROUP BY tenant_id, fecha, canal
),

-- -----------------------------------------------------------------------
-- 5. Top 5 categorias por dia + tenant + canal
-- -----------------------------------------------------------------------
category_ranked AS (
    SELECT
        o.tenant_id,
        o.fecha,
        o.canal,
        pc.category_id,
        SUM(i.line_total_net)                                                   AS cat_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY o.tenant_id, o.fecha, o.canal
            ORDER BY SUM(i.line_total_net) DESC
        )                                                                       AS cat_rank
    FROM orders o
    LEFT JOIN items i ON o.order_id = i.order_id AND o.tenant_id = i.tenant_id
    JOIN {{ ref('dim_product_category') }} pc
        ON  i.sku        = pc.sku
        AND o.tenant_id  = pc.tenant_id
    WHERE pc.category_id IS NOT NULL
    GROUP BY o.tenant_id, o.fecha, o.canal, pc.category_id
),

top_categories AS (
    SELECT
        tenant_id,
        fecha,
        canal,
        TO_JSON_STRING(
            ARRAY_AGG(
                STRUCT(category_id, cat_revenue)
                ORDER BY cat_rank
            )
        )                                                                       AS top_categories
    FROM category_ranked
    WHERE cat_rank <= 5
    GROUP BY tenant_id, fecha, canal
),

-- -----------------------------------------------------------------------
-- 6. Join todo
-- -----------------------------------------------------------------------
joined AS (
    SELECT
        om.tenant_id,
        om.fecha,
        om.canal,
        om.total_orders,
        om.total_revenue,
        om.avg_ticket,
        om.orders_completed,
        om.orders_canceled,
        COALESCE(im.total_items, 0)                                             AS total_items,
        ts.top_skus,
        tc.top_categories
    FROM order_metrics om
    LEFT JOIN item_metrics im
        ON om.tenant_id = im.tenant_id AND om.fecha = im.fecha AND om.canal = im.canal
    LEFT JOIN top_skus ts
        ON om.tenant_id = ts.tenant_id AND om.fecha = ts.fecha AND om.canal = ts.canal
    LEFT JOIN top_categories tc
        ON om.tenant_id = tc.tenant_id AND om.fecha = tc.fecha AND om.canal = tc.canal
)

-- -----------------------------------------------------------------------
-- 7. Output final
-- -----------------------------------------------------------------------
SELECT
    TO_HEX(SHA256(CONCAT(tenant_id, CAST(fecha AS STRING), canal)))            AS kpi_id,
    tenant_id,
    fecha,
    canal,
    total_orders,
    total_revenue,
    avg_ticket,
    total_items,
    orders_completed,
    orders_canceled,
    top_skus,
    top_categories,
    CURRENT_DATETIME('UTC')                                                     AS last_updated
FROM joined
