-- models/staging/aatn/stg_aatn__products.sql
-- Normaliza catálogo de productos Ariztía desde bronze.ecommerce (entity='products')
-- Fuente: Endpoint de Magento — extrae metadatos base y desanida custom_attributes.
-- Materialización: view — parseo y extracción de JSON sin deduplicación.

{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'products'
      AND tenant_id = 'aatn'
)

SELECT
    bronze_id,
    tenant_id,
    ingested_at,

    JSON_VALUE(raw_json, '$.sku')                                       AS sku,
    JSON_VALUE(raw_json, '$.name')                                      AS sku_name,
    JSON_VALUE(raw_json, '$.type_id')                                   AS product_type,
    CAST(JSON_VALUE(raw_json, '$.status') AS INT64)                     AS catalog_status,

    -- Extrae valores dinámicos desde el arreglo custom_attributes de Magento
    (
        SELECT JSON_VALUE(attr, '$.value')
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.custom_attributes')) AS attr
        WHERE JSON_VALUE(attr, '$.attribute_code') = 'marca'
    )                                                                   AS brand,

    (
        SELECT JSON_VALUE(attr, '$.value')
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.custom_attributes')) AS attr
        WHERE JSON_VALUE(attr, '$.attribute_code') = 'descripcion_categoria'
    )                                                                   AS category_name,

    (
        SELECT JSON_VALUE(attr, '$.value')
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.custom_attributes')) AS attr
        WHERE JSON_VALUE(attr, '$.attribute_code') = 'descripcion_subcategoria'
    )                                                                   AS subcategory_name,

    -- Fechas
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', JSON_VALUE(raw_json, '$.created_at')) AS created_at,
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', JSON_VALUE(raw_json, '$.updated_at')) AS updated_at

FROM bronze
WHERE JSON_VALUE(raw_json, '$.sku') IS NOT NULL
