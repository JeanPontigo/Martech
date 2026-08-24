-- models/staging/aatn/stg_aatn__order_items.sql
-- Normaliza líneas de orden Ariztía desde bronze.ecommerce (entity='orders')
-- Fuente: 100% endpoint custom — items[] anidados dentro de cada orden.
-- No requiere overlay: una vez creada, una orden en Ariztía no se modifica
-- (confirmado — si falta algo, se crea una orden nueva, no se edita la
-- existente). Una sola fuente basta, a diferencia de fact_orders.
-- Materialización: view — solo parseo/UNNEST, sin dedupe.

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

    JSON_VALUE(raw_json, '$.order_id')                                              AS order_id,

    JSON_VALUE(item, '$.sku')                                                       AS sku,

    -- PENDIENTE DE CONFIRMAR: ¿existe equivalente a product_type en el
    -- custom de Ariztía? Se deja NULL hasta confirmar.
    CAST(NULL AS STRING)                                                            AS product_type,

    CAST(JSON_VALUE(item, '$.cantidad') AS FLOAT64)                                 AS qty_ordered,

    -- Sin dato en el custom — no asumir equivalencia con qty_ordered.
    CAST(NULL AS FLOAT64)                                                           AS qty_canceled,
    CAST(NULL AS FLOAT64)                                                           AS qty_invoiced,
    CAST(NULL AS FLOAT64)                                                           AS qty_shipped,

    CAST(JSON_VALUE(item, '$.price')      AS FLOAT64)                              AS unit_price,

    -- original_price no existe en el custom de Ariztía (confirmado) —
    -- sin este campo no se puede derivar list_price_discounted igual que pf.
    CAST(NULL AS FLOAT64)                                                          AS original_price,

    CAST(JSON_VALUE(item, '$.venta_neta') AS FLOAT64)                              AS line_total_net,

    -- PENDIENTE: monto_descuento/porcentaje_descuento en validación con TI
    -- de Ariztía (siempre 0 en la muestra revisada, semántica no confirmada).
    CAST(NULL AS FLOAT64)                                                          AS discount_amount,
    CAST(NULL AS FLOAT64)                                                          AS discount_pct,

    -- Sin original_price ni discount_amount confirmados, ambos booleanos
    -- quedan FALSE por defecto (COALESCE evita NULL en comparaciones).
    COALESCE(
    SAFE_CAST(JSON_VALUE(item, '$.price') AS FLOAT64),
    0.0
    )                                                                                AS list_price_discounted,

    COALESCE(
        CAST(NULL AS FLOAT64) > 0,
        FALSE
    )                                                                                AS discount_applied,

    -- fecha_compra + hora_compra ya vienen en hora Chile local — mismo
    -- criterio que stg_aatn__orders_custom.
    DATETIME(
        CONCAT(JSON_VALUE(raw_json, '$.fecha_compra'), ' ', JSON_VALUE(raw_json, '$.hora_compra'))
    )                                                                                AS created_at,
    DATETIME(
        CONCAT(JSON_VALUE(raw_json, '$.fecha_compra'), ' ', JSON_VALUE(raw_json, '$.hora_compra'))
    )                                                                                AS updated_at

FROM unnested
WHERE JSON_VALUE(raw_json, '$.order_id') IS NOT NULL
