-- models/gold/kpi_orders_daily.sql
-- Fuente: silver.fact_orders + silver.fact_order_items + silver.dim_product
-- Granularidad: una fila por dia + tenant + canal
-- Incluye top 5 SKUs y categorias del dia en formato JSON
--
-- Criterio de venta: total_orders, total_revenue, avg_ticket, total_items y
-- los rankings consideran SOLO ordenes con status_norm = 'pagada'.
-- Las ordenes canceladas, pendientes y sin estado se reportan aparte como
-- conteo, sin sumar a la venta.
--
-- total_revenue y avg_ticket usan revenue_net (subtotal menos descuento),
-- no subtotal_net, que viene bruto en el header de Magento.
-- Los rankings de SKU y categoria usan line_total_net, que ya viene neto.

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Leer Silver — todas las ordenes, con su estado normalizado
-- -----------------------------------------------------------------------
orders AS (
    SELECT
        order_id,
        tenant_id,
        DATE(created_at_chile)  AS fecha,
        canal_compra            AS canal,
        status,
        status_norm,
        revenue_net
    FROM {{ ref('fact_orders') }}
),

-- Solo las que cuentan como venta — base de los rankings y los items
orders_pagadas AS (
    SELECT *
    FROM orders
    WHERE status_norm = 'pagada'
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

        -- Venta efectiva
        COUNT(DISTINCT IF(status_norm = 'pagada', order_id, NULL))              AS total_orders,
        SUM(IF(status_norm = 'pagada', revenue_net, 0))                         AS total_revenue,
        ROUND(SAFE_DIVIDE(
            SUM(IF(status_norm = 'pagada', revenue_net, 0)),
            COUNT(DISTINCT IF(status_norm = 'pagada', order_id, NULL))
        ), 2)                                                                   AS avg_ticket,

        -- Ordenes que no se concretaron
        COUNT(DISTINCT IF(status_norm = 'cancelada',   order_id, NULL))         AS orders_canceled,
        COUNT(DISTINCT IF(status_norm = 'pendiente',   order_id, NULL))         AS orders_pending,
        COUNT(DISTINCT IF(status_norm = 'devuelta',    order_id, NULL))         AS orders_returned,
        COUNT(DISTINCT IF(status_norm = 'sin_estado',  order_id, NULL))         AS orders_sin_estado,

        -- Total bruto, para calcular tasa de conversion
        COUNT(DISTINCT order_id)                                                AS total_orders_brutas
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
    FROM orders_pagadas o
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
    FROM orders_pagadas o
    JOIN items i ON o.order_id = i.order_id AND o.tenant_id = i.tenant_id
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
        p.category_name,
        SUM(i.line_total_net)                                                   AS cat_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY o.tenant_id, o.fecha, o.canal
            ORDER BY SUM(i.line_total_net) DESC
        )                                                                       AS cat_rank
    FROM orders_pagadas o
    JOIN items i ON o.order_id = i.order_id AND o.tenant_id = i.tenant_id
    LEFT JOIN {{ ref('dim_product') }} p
        ON  i.sku        = p.sku
        AND o.tenant_id  = p.tenant_id
    WHERE p.category_name IS NOT NULL
    GROUP BY o.tenant_id, o.fecha, o.canal, p.category_name
),

top_categories AS (
    SELECT
        tenant_id,
        fecha,
        canal,
        TO_JSON_STRING(
            ARRAY_AGG(
                STRUCT(category_name, cat_revenue)
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
        om.orders_canceled,
        om.orders_pending,
        om.orders_returned,
        om.orders_sin_estado,
        om.total_orders_brutas,
        ROUND(SAFE_DIVIDE(om.total_orders, om.total_orders_brutas), 4)          AS tasa_concrecion,
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
    orders_canceled,
    orders_pending,
    orders_returned,
    orders_sin_estado,
    total_orders_brutas,
    tasa_concrecion,
    top_skus,
    top_categories,
    CURRENT_DATETIME('UTC')                                                     AS last_updated
FROM joined
