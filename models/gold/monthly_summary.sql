-- models/gold/monthly_summary.sql
-- Fuente: silver.fact_orders
-- Granularidad: una fila por mes + tenant
-- Métricas agregadas con variaciones mes a mes
{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}
WITH base AS (
    SELECT
        tenant_id,
        FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(created_at), MONTH)) AS mes,
        MIN(created_at)                                  AS created_at,
        COUNT(DISTINCT client_id)                        AS clientes_activos,
        COUNT(DISTINCT order_id)                         AS transacciones,
        ROUND(COUNT(DISTINCT order_id) / COUNT(DISTINCT client_id), 2) AS tx_por_cliente,
        ROUND(SUM(subtotal_net), 0)                      AS revenue,
        ROUND(SUM(subtotal_net) / COUNT(DISTINCT client_id), 0) AS revenue_por_cliente,
        ROUND(SUM(subtotal_net) / COUNT(DISTINCT order_id), 0)  AS ticket_promedio
    FROM {{ ref('fact_orders') }}
    GROUP BY tenant_id, FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(created_at), MONTH))
),
variaciones AS (
    SELECT
        tenant_id,
        mes, created_at, clientes_activos, transacciones, tx_por_cliente,
        revenue, revenue_por_cliente, ticket_promedio,
        ROUND(SAFE_DIVIDE(clientes_activos - LAG(clientes_activos) OVER (PARTITION BY tenant_id ORDER BY mes), LAG(clientes_activos) OVER (PARTITION BY tenant_id ORDER BY mes)), 3) AS var_clientes,
        ROUND(SAFE_DIVIDE(transacciones - LAG(transacciones) OVER (PARTITION BY tenant_id ORDER BY mes), LAG(transacciones) OVER (PARTITION BY tenant_id ORDER BY mes)), 3) AS var_transacciones,
        ROUND(SAFE_DIVIDE(revenue - LAG(revenue) OVER (PARTITION BY tenant_id ORDER BY mes), LAG(revenue) OVER (PARTITION BY tenant_id ORDER BY mes)), 3) AS var_revenue,
        ROUND(SAFE_DIVIDE(ticket_promedio - LAG(ticket_promedio) OVER (PARTITION BY tenant_id ORDER BY mes), LAG(ticket_promedio) OVER (PARTITION BY tenant_id ORDER BY mes)), 3) AS var_ticket,
        ROUND(SAFE_DIVIDE(tx_por_cliente - LAG(tx_por_cliente) OVER (PARTITION BY tenant_id ORDER BY mes), LAG(tx_por_cliente) OVER (PARTITION BY tenant_id ORDER BY mes)), 3) AS var_tx_por_cliente,
        ROUND(SAFE_DIVIDE(revenue_por_cliente - LAG(revenue_por_cliente) OVER (PARTITION BY tenant_id ORDER BY mes), LAG(revenue_por_cliente) OVER (PARTITION BY tenant_id ORDER BY mes)), 3) AS var_revenue_por_cliente
    FROM base
)
SELECT
    tenant_id,
    mes, created_at, clientes_activos, transacciones, tx_por_cliente,
    revenue, revenue_por_cliente, ticket_promedio,
    CASE WHEN var_revenue IS NULL THEN 'S/C' WHEN var_revenue >= 0 THEN CONCAT('▲ ', FORMAT('%.1f', var_revenue * 100), '%') ELSE CONCAT('▼ ', FORMAT('%.1f', ABS(var_revenue * 100)), '%') END AS var_revenue_texto,
    CASE WHEN var_clientes IS NULL THEN 'S/C' WHEN var_clientes >= 0 THEN CONCAT('▲ ', FORMAT('%.1f', var_clientes * 100), '%') ELSE CONCAT('▼ ', FORMAT('%.1f', ABS(var_clientes * 100)), '%') END AS var_clientes_texto,
    CASE WHEN var_transacciones IS NULL THEN 'S/C' WHEN var_transacciones >= 0 THEN CONCAT('▲ ', FORMAT('%.1f', var_transacciones * 100), '%') ELSE CONCAT('▼ ', FORMAT('%.1f', ABS(var_transacciones * 100)), '%') END AS var_transacciones_texto,
    CASE WHEN var_ticket IS NULL THEN 'S/C' WHEN var_ticket >= 0 THEN CONCAT('▲ ', FORMAT('%.1f', var_ticket * 100), '%') ELSE CONCAT('▼ ', FORMAT('%.1f', ABS(var_ticket * 100)), '%') END AS var_ticket_texto,
    CASE WHEN var_tx_por_cliente IS NULL THEN 'S/C' WHEN var_tx_por_cliente >= 0 THEN CONCAT('▲ ', FORMAT('%.1f', var_tx_por_cliente * 100), '%') ELSE CONCAT('▼ ', FORMAT('%.1f', ABS(var_tx_por_cliente * 100)), '%') END AS var_tx_por_cliente_texto,
    CASE WHEN var_revenue_por_cliente IS NULL THEN 'S/C' WHEN var_revenue_por_cliente >= 0 THEN CONCAT('▲ ', FORMAT('%.1f', var_revenue_por_cliente * 100), '%') ELSE CONCAT('▼ ', FORMAT('%.1f', ABS(var_revenue_por_cliente * 100)), '%') END AS var_revenue_por_cliente_texto
FROM variaciones
