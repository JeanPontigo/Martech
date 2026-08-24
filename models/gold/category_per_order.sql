-- models/gold/category_per_order.sql
-- Fuente: silver.fact_orders + silver.fact_order_items + silver.dim_product
-- Granularidad: una fila por mes + tenant
-- Categorías, SKUs y unidades promedio por orden
{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}
WITH categorias_por_orden AS (
    SELECT
        o.order_id,
        o.tenant_id,
        o.client_id,
        o.created_at,
        o.subtotal_net,
        FORMAT_DATE('%Y-%m', DATE(o.created_at))        AS mes,
        COUNT(DISTINCT p.category_name)                  AS categorias_distintas,
        COUNT(DISTINCT oi.sku)                           AS sku_distintos,
        SUM(oi.qty_ordered)                              AS unidades
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    LEFT JOIN {{ ref('dim_product') }} p
        ON oi.sku = p.sku AND oi.tenant_id = p.tenant_id
    GROUP BY o.order_id, o.tenant_id, o.client_id, o.created_at, o.subtotal_net,
             FORMAT_DATE('%Y-%m', DATE(o.created_at))
)
SELECT
    tenant_id,
    mes,
    MIN(created_at)                                      AS created_at,
    COUNT(*)                                             AS ordenes,
    ROUND(AVG(categorias_distintas), 2)                  AS categorias_por_orden,
    ROUND(AVG(sku_distintos), 2)                         AS sku_por_orden,
    ROUND(AVG(unidades), 2)                              AS unidades_por_orden,
    ROUND(SUM(subtotal_net) / COUNT(DISTINCT order_id), 0) AS ticket
FROM categorias_por_orden
GROUP BY tenant_id, mes
