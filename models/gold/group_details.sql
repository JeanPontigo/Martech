-- models/gold/order_client_detail.sql
-- Fuente: silver.fact_orders + silver.dim_client
-- Granularidad: una fila por orden con detalle del cliente

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
    o.created_at,
    o.subtotal_net                  AS subtotal,
    o.status                        AS estado,
    c.client_id,
    c.first_name,
    c.last_name,
    c.group_id,
    c.group_name
FROM {{ ref('fact_orders') }} o
LEFT JOIN {{ ref('dim_client') }} c
    ON o.client_id = c.client_id
    AND o.tenant_id = c.tenant_id
