-- models/silver/dim_product_category.sql
-- Fuente: bronze.ecommerce WHERE entity = 'products'
--         + silver.dim_category como referencia de niveles
-- Tenant: todos (MVP: pf)
-- Granularidad: una fila por combinación producto-categoría (N:N)
-- Materialización: table — full load siempre
-- PK: tenant_id + category_id

{{
    config(
        materialized='table'
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Leer Bronze — productos
-- -----------------------------------------------------------------------
bronze AS (
    SELECT
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'products'
),

-- -----------------------------------------------------------------------
-- 2. Aplanar todos los nodos del árbol (niveles 2, 3 y 4)
--    para poder remapear nivel 4 → parent_id (nivel 3)
-- -----------------------------------------------------------------------
nivel_2 AS (
    SELECT
        tenant_id,
        JSON_VALUE(cat2, '$.id')        AS node_id,
        JSON_VALUE(cat2, '$.parent_id') AS node_parent_id,
        CAST(JSON_VALUE(cat2, '$.level') AS INT64) AS node_level
    FROM {{ source('bronze', 'ecommerce') }},
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2
    WHERE entity = 'categories'
),

nivel_3 AS (
    SELECT
        tenant_id,
        JSON_VALUE(cat3, '$.id')        AS node_id,
        JSON_VALUE(cat3, '$.parent_id') AS node_parent_id,
        CAST(JSON_VALUE(cat3, '$.level') AS INT64) AS node_level
    FROM {{ source('bronze', 'ecommerce') }},
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2,
    UNNEST(JSON_QUERY_ARRAY(cat2, '$.children_data'))     AS cat3
    WHERE entity = 'categories'
),

nivel_4 AS (
    SELECT
        tenant_id,
        JSON_VALUE(cat4, '$.id')        AS node_id,
        JSON_VALUE(cat4, '$.parent_id') AS node_parent_id,
        CAST(JSON_VALUE(cat4, '$.level') AS INT64) AS node_level
    FROM {{ source('bronze', 'ecommerce') }},
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2,
    UNNEST(JSON_QUERY_ARRAY(cat2, '$.children_data'))     AS cat3,
    UNNEST(JSON_QUERY_ARRAY(cat3, '$.children_data'))     AS cat4
    WHERE entity = 'categories'
),

-- -----------------------------------------------------------------------
-- 3. Unir todos los nodos en una sola tabla de referencia
-- -----------------------------------------------------------------------
all_nodes AS (
    SELECT * FROM nivel_2
    UNION ALL
    SELECT * FROM nivel_3
    UNION ALL
    SELECT * FROM nivel_4
),

-- -----------------------------------------------------------------------
-- 4. Extraer category_ids de cada producto (UNNEST del array)
-- -----------------------------------------------------------------------
product_categories_raw AS (
    SELECT
        b.tenant_id,
        JSON_VALUE(b.raw_json, '$.sku')     AS sku,
        JSON_VALUE(cat_id)                  AS category_id_raw
    FROM bronze AS b,
    UNNEST(
        JSON_QUERY_ARRAY(
            (
                SELECT JSON_QUERY(attr, '$.value')
                FROM UNNEST(JSON_QUERY_ARRAY(b.raw_json, '$.custom_attributes')) AS attr
                WHERE JSON_VALUE(attr, '$.attribute_code') = 'category_ids'
                LIMIT 1
            )
        )
    ) AS cat_id
    WHERE JSON_VALUE(b.raw_json, '$.sku') IS NOT NULL
),

-- -----------------------------------------------------------------------
-- 5. Remapear nivel 4 → parent_id (nivel 3)
--    Si el category_id no existe en all_nodes o es nivel <= 3, se usa directo
--    Si es nivel 4, se sube al parent_id correspondiente
-- -----------------------------------------------------------------------
remapped AS (
    SELECT
        pc.tenant_id,
        pc.sku,
        CASE
            WHEN n.node_level = 4 THEN n.node_parent_id
            ELSE pc.category_id_raw
        END                                 AS category_id
    FROM product_categories_raw AS pc
    LEFT JOIN all_nodes AS n
        ON  pc.tenant_id     = n.tenant_id
        AND pc.category_id_raw = n.node_id
),

-- -----------------------------------------------------------------------
-- 6. Filtrar solo category_ids que existen en dim_category (niveles 2 y 3)
--    y deduplicar combinaciones producto-categoría
-- -----------------------------------------------------------------------
filtered AS (
    SELECT DISTINCT
        r.tenant_id,
        r.sku,
        r.category_id
    FROM remapped AS r
    INNER JOIN {{ ref('dim_category') }} AS dc
        ON  r.tenant_id   = dc.tenant_id
        AND r.category_id = dc.category_id
    WHERE r.category_id IS NOT NULL
)

-- -----------------------------------------------------------------------
-- 7. Output final
-- -----------------------------------------------------------------------
SELECT
    tenant_id,
    category_id,
    sku,
FROM filtered