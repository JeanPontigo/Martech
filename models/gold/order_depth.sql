-- models/gold/order_depth.sql
-- Fuente: silver.fact_orders + silver.fact_order_items
-- Granularidad: una fila por mes + tenant
-- Profundidad de orden: unidades, SKUs y ticket promedio
--
-- Todas las metricas de dinero salen de line_total_net (nivel item), que ya
-- viene neto de descuento. El modelo no lee columnas de dinero de fact_orders,
-- asi que revenue / transacciones = ticket_promedio por construccion.
--
-- unidades_promedio_por_orden excluye las lineas product_type = 'bundle'.
-- En Magento el padre del pack lleva el dinero y como cantidad el numero de
-- packs, mientras los hijos llevan las unidades reales con monto cero.
-- Sumar ambos contaba de mas. Solo aplica a mc: pf no tiene packs y el
-- endpoint custom de aatn no entrega product_type.
-- El dinero NO se toca: el padre aporta el monto y los hijos aportan cero.

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
        o.created_at_chile,
        FORMAT_DATE('%Y-%m', DATE(o.created_at_chile))   AS mes,
        SUM(IF(COALESCE(oi.product_type, '') = 'bundle', 0, oi.qty_ordered))    AS unidades_orden,
        COUNT(DISTINCT oi.sku)                           AS sku_distintos_orden,
        SUM(oi.line_total_net)                           AS revenue_orden
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    WHERE o.status_norm = 'pagada'
    GROUP BY o.order_id, o.tenant_id, o.client_id, o.created_at_chile,
             FORMAT_DATE('%Y-%m', DATE(o.created_at_chile))
)
SELECT
    tenant_id,
    mes,
    MIN(created_at_chile)                                                       AS created_at_chile,
    COUNT(DISTINCT order_id)                                                    AS transacciones,
    COUNT(DISTINCT client_id)                                                   AS clientes_activos,
    ROUND(AVG(unidades_orden), 2)                                               AS unidades_promedio_por_orden,
    ROUND(AVG(sku_distintos_orden), 2)                                          AS sku_promedio_por_orden,
    ROUND(SUM(revenue_orden), 0)                                                AS revenue,
    ROUND(SAFE_DIVIDE(SUM(revenue_orden), COUNT(DISTINCT client_id)), 0)        AS revenue_por_cliente,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT order_id), COUNT(DISTINCT client_id)), 2)  AS tx_por_cliente,
    ROUND(SAFE_DIVIDE(SUM(revenue_orden), COUNT(DISTINCT order_id)), 0)         AS ticket_promedio
FROM profundidad_orden
GROUP BY tenant_id, mes
