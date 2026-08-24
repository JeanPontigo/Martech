-- models/staging/pf/stg_pf__orders.sql
-- Normaliza órdenes PF desde bronze.ecommerce (entity IN 'orders','orders_updated')
-- Fuente: 100% nativo Magento — misma forma de JSON en ambas entidades.
-- Materialización: view — solo parseo de JSON, sin dedupe ni lógica de negocio
-- (el dedupe y el bridge viven en silver.fact_orders).

{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity IN ('orders', 'orders_updated')
      AND tenant_id = 'pf'
)

SELECT
    bronze_id,
    tenant_id,
    ingested_at,

    JSON_VALUE(raw_json, '$.entity_id')                                             AS order_id,
    JSON_VALUE(raw_json, '$.increment_id')                                          AS incremental_id,
    JSON_VALUE(raw_json, '$.customer_id')                                           AS client_id,
    JSON_VALUE(raw_json, '$.customer_email')                                        AS customer_email,
    JSON_VALUE(raw_json, '$.extension_attributes.amcompany_attributes.company_id')  AS company_id,

    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))                       AS created_at,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))                       AS updated_at,

    JSON_VALUE(raw_json, '$.status')                                                 AS status,

    CAST(JSON_VALUE(raw_json, '$.subtotal')         AS FLOAT64)                     AS subtotal_net,
    CAST(JSON_VALUE(raw_json, '$.discount_amount')  AS FLOAT64)                     AS discount_amount,
    CAST(JSON_VALUE(raw_json, '$.shipping_amount')  AS FLOAT64)                     AS shipping_amount,
    JSON_VALUE(raw_json, '$.extension_attributes.coupon_code')                      AS coupon_code,
    LOWER(JSON_VALUE(raw_json, '$.payment.method'))                                 AS medio_pago,
    'ecommerce'                                                                      AS canal_compra,

    INITCAP(JSON_VALUE(
        raw_json,
        '$.extension_attributes.shipping_assignments[0].shipping.address.region'
    ))                                                                               AS region_compra,
    INITCAP(JSON_VALUE(
        raw_json,
        '$.extension_attributes.shipping_assignments[0].shipping.address.city'
    ))                                                                               AS ciudad_compra,

    CAST(NULL AS STRING)                                                             AS discount_pct,
    CAST(NULL AS STRING)                                                             AS courier,
    CAST(NULL AS STRING)                                                             AS erp_id,
    CAST(NULL AS STRING)                                                             AS gifcard_code

FROM bronze