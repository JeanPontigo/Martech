-- models/gold/order_depth.sql
-- Fuente: silver.fact_orders + silver.fact_order_items
-- Granularidad: una fila por mes + tenant
-- Profundidad de orden: unidades, SKUs y ticket promedio

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH profundidad_orden AS (
    SELECT
        o.order_id,
        o.tenant_id,
        o.client_id,
        o.created_at,
        o.subtotal_net,
        FORMAT_DATE('%Y-%m', DATE(o.created_at))        AS mes,
        SUM(oi.qty_ordered)                              AS unidades_orden,
        COUNT(DISTINCT oi.sku)                           AS sku_distintos_orden,
        SUM(oi.line_total_net)                           AS revenue_orden
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    GROUP BY o.order_id, o.tenant_id, o.client_id, o.created_at, o.subtotal_net,
             FORMAT_DATE('%Y-%m', DATE(o.created_at))
)
SELECT
    tenant_id,
    mes,
    MIN(created_at)                                      AS created_at,
    COUNT(DISTINCT order_id)                             AS transacciones,
    COUNT(DISTINCT client_id)                            AS clientes_activos,
    ROUND(AVG(unidades_orden), 2)                        AS unidades_promedio_por_orden,
    ROUND(AVG(sku_distintos_orden), 2)                   AS sku_promedio_por_orden,
    ROUND(SUM(revenue_orden), 0)                         AS revenue,
    ROUND(SUM(subtotal_net) / COUNT(DISTINCT order_id), 0) AS ticket_promedio
FROM profundidad_orden
GROUP BY tenant_id, mes
