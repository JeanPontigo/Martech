-- models/staging/mc/stg_mc__orders.sql
-- Normaliza órdenes Carozzi desde bronze.ecommerce (entity IN 'orders','orders_updated')
-- Fuente: 100% nativo Magento — B2C (sin companies ni B2B attributes)
-- Status: se traduce al español en este staging para que la columna sea
-- comparable con aatn y pf, que ya vienen en español del origen.
-- 'complete' y 'Recibido_Odoo' son los estados válidos de Carozzi.
-- La categoria analitica (pagada/cancelada/pendiente) la define status_norm
-- en fact_orders, no este modelo.
-- Materialización: view — solo parseo, sin dedupe
{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity IN ('orders', 'orders_updated')
      AND tenant_id = 'mc'
)
SELECT
    bronze_id,
    tenant_id,
    ingested_at,
    JSON_VALUE(raw_json, '$.entity_id')                                             AS order_id,
    JSON_VALUE(raw_json, '$.increment_id')                                          AS incremental_id,
    JSON_VALUE(raw_json, '$.customer_id')                                           AS client_id,
    JSON_VALUE(raw_json, '$.customer_email')                                        AS customer_email,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))                       AS created_at,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))                       AS updated_at,
    -- Status de Carozzi: 'complete' → 'completado', 'Recibido_Odoo' → 'completado'
    CASE JSON_VALUE(raw_json, '$.status')
        WHEN 'complete'       THEN 'completado'
        WHEN 'Recibido_Odoo'  THEN 'completado'
        WHEN 'canceled'       THEN 'cancelado'
        WHEN 'pending'        THEN 'pendiente'
        WHEN 'processing'     THEN 'procesando'
        ELSE JSON_VALUE(raw_json, '$.status')
    END                                                                             AS status,
    CAST(JSON_VALUE(raw_json, '$.subtotal')         AS FLOAT64)                     AS subtotal_net,
    CAST(JSON_VALUE(raw_json, '$.discount_amount')  AS FLOAT64)                     AS discount_amount,
    CAST(JSON_VALUE(raw_json, '$.shipping_amount')  AS FLOAT64)                     AS shipping_amount,
    -- discount_pct se deriva en fact_orders desde discount_amount y
    -- subtotal_net, para que sea una sola definicion en los tres tenants.
    CAST(NULL AS STRING)                                                            AS discount_pct,
    JSON_VALUE(raw_json, '$.extension_attributes.coupon_code')                      AS coupon_code,
    LOWER(JSON_VALUE(raw_json, '$.payment.method'))                                 AS medio_pago,
    'ecommerce'                                                                     AS canal_compra,
    NULL                                                                            AS courier,
    INITCAP(JSON_VALUE(
        raw_json,
        '$.extension_attributes.shipping_assignments[0].shipping.address.region'
    ))                                                                              AS region_compra,
    INITCAP(JSON_VALUE(
        raw_json,
        '$.extension_attributes.shipping_assignments[0].shipping.address.city'
    ))                                                                              AS ciudad_compra,
    -- RUT del cliente desde extension_attributes (específico de Carozzi)
    JSON_VALUE(raw_json, '$.extension_attributes.customer_rut')                     AS rut_cliente,
    NULL                                                                            AS erp_id,
    NULL                                                                            AS gifcard_code
FROM bronze
