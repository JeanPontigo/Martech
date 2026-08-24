-- models/staging/mc/stg_mc__order_items.sql
-- Normaliza líneas de orden Carozzi desde bronze.ecommerce (entity IN 'orders','orders_updated')
-- Fuente: 100% nativo Magento — items[] anidados en cada orden.
-- Materialización: view — solo parseo/UNNEST, sin dedupe.
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
),
unnested AS (
    SELECT
        bronze_id,
        tenant_id,
        ingested_at,
        raw_json,
        item
    FROM bronze,
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.items')) AS item
)
SELECT
    bronze_id,
    tenant_id,
    ingested_at,
    JSON_VALUE(raw_json, '$.entity_id')                                             AS order_id,
    JSON_VALUE(item, '$.sku')                                                       AS sku,
    JSON_VALUE(item, '$.product_type')                                              AS product_type,
    CAST(JSON_VALUE(item, '$.qty_ordered')  AS FLOAT64)                             AS qty_ordered,
    CAST(JSON_VALUE(item, '$.qty_canceled') AS FLOAT64)                             AS qty_canceled,
    CAST(JSON_VALUE(item, '$.qty_invoiced') AS FLOAT64)                             AS qty_invoiced,
    CAST(JSON_VALUE(item, '$.qty_shipped')  AS FLOAT64)                             AS qty_shipped,
    CAST(JSON_VALUE(item, '$.price')           AS FLOAT64)                          AS unit_price,
    CAST(JSON_VALUE(item, '$.original_price')  AS FLOAT64)                          AS original_price,
    CAST(JSON_VALUE(item, '$.row_total')       AS FLOAT64)                          AS line_total_net,
    CAST(JSON_VALUE(item, '$.discount_amount') AS FLOAT64)                          AS discount_amount,
    CAST(NULL AS FLOAT64)                                                           AS discount_pct,
    COALESCE(
        CAST(JSON_VALUE(item, '$.price') AS FLOAT64), 
        SAFE_CAST(JSON_VALUE(item, '$.price') AS FLOAT64),
        0.0
    )                                                                               AS list_price_discounted,
    COALESCE(
        CAST(JSON_VALUE(item, '$.discount_amount') AS FLOAT64) > 0,
        FALSE
    )                                                                               AS discount_applied,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))                       AS created_at,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))                       AS updated_at
FROM unnested
