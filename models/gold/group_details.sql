-- models/gold/group_details.sql
-- Fuente: silver.fact_orders + silver.dim_client
-- Granularidad: una fila por orden con detalle del cliente
--
-- Modelo de detalle: expone el bruto, el descuento y el neto por separado.
-- subtotal viene bruto desde el header de Magento; total_neto es lo pagado.
-- Para sumar ventas usar total_neto, nunca subtotal.

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

SELECT
    o.tenant_id,
    o.order_id,
    o.created_at_chile,
    o.subtotal_net                  AS subtotal,
    o.discount_amount               AS descuento,
    o.revenue_net                   AS total_neto,
    o.status                        AS estado,
    o.status_norm                   AS status_norm,
    c.client_id,
    c.first_name,
    c.last_name,
    c.group_id,
    c.group_name
FROM {{ ref('fact_orders') }} o
LEFT JOIN {{ ref('dim_client') }} c
    ON o.client_id = c.client_id
    AND o.tenant_id = c.tenant_id
