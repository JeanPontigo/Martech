-- models/staging/aatn/stg_aatn__orders_custom.sql
-- Normaliza órdenes Ariztía desde bronze.ecommerce (entity='orders')
-- Fuente: 100% endpoint custom — fuente de verdad para identidad y detalle
-- (company_id, client_id, centro, comuna, sap_id). NO incluye status
-- actualizado — eso vive en stg_aatn__orders_status.sql (overlay).
-- Materialización: view — solo parseo de JSON, sin dedupe.

{{ config(materialized='view') }}
WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'orders'
      AND tenant_id = 'aatn'
)
SELECT
    bronze_id,
    tenant_id,
    ingested_at,
    JSON_VALUE(raw_json, '$.order_id')                                              AS order_id,
    JSON_VALUE(raw_json, '$.sap_id')                                                AS incremental_id,
    JSON_VALUE(raw_json, '$.customer_sap_id')                                       AS client_id,
    JSON_VALUE(raw_json, '$.email')                                                 AS customer_email,
    JSON_VALUE(raw_json, '$.company_id')                                            AS company_id,
    DATETIME(
        CONCAT(JSON_VALUE(raw_json, '$.fecha_compra'), ' ', JSON_VALUE(raw_json, '$.hora_compra'))
    )                                                                                AS created_at_local,
    JSON_VALUE(raw_json, '$.status')                                                 AS status,
    INITCAP(JSON_VALUE(raw_json, '$.comuna'))                                        AS ciudad_compra,
    (
        SELECT SUM(CAST(JSON_VALUE(item, '$.venta_neta') AS FLOAT64))
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.items')) AS item
    )                                                                                AS subtotal_net,
    CAST(NULL AS FLOAT64)                                                            AS discount_amount,
    CAST(NULL AS STRING)                                                             AS discount_pct,
    CAST(NULL AS FLOAT64)                                                            AS shipping_amount,
    CAST(NULL AS STRING)                                                             AS courier,
    (
        SELECT JSON_VALUE(item, '$.nombre_cupon')
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.items')) AS item
        WHERE JSON_VALUE(item, '$.nombre_cupon') IS NOT NULL
        LIMIT 1
    )                                                                                AS coupon_code,
    CAST(NULL AS STRING)                                                             AS medio_pago,
    cr.region                                                                        AS region_compra,
    JSON_VALUE(raw_json, '$.sap_id')                                                 AS erp_id,
    CAST(NULL AS STRING)                                                             AS gifcard_code,
    'ecommerce'                                                                      AS canal_compra
FROM bronze
LEFT JOIN {{ ref('comunas_regiones') }} cr
    ON {{ normalize_string("SPLIT(JSON_VALUE(raw_json, '$.comuna'), ' - ')[OFFSET(0)]") }}
     = {{ normalize_string('cr.comunas') }}
