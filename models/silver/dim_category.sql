-- models/silver/dim_category.sql
-- Fuente: bronze.ecommerce WHERE entity = 'categories'
-- Tenant: todos (MVP: pf)
-- Granularidad: una fila por categoría (niveles 2 y 3 únicamente)
-- Materialización: table — full load siempre (catálogo estable)
-- PK: tenant_id + category_id

{{
    config(
        materialized='table'
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Leer Bronze — última ingesta por tenant
-- -----------------------------------------------------------------------
bronze AS (
    SELECT
        id          AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'categories'
),

-- -----------------------------------------------------------------------
-- 2. Aplanar nivel 2 (categorías) desde children_data de la raíz
-- -----------------------------------------------------------------------
nivel_2 AS (
    SELECT
        bronze_id,
        tenant_id,
        ingested_at,
        JSON_VALUE(cat2, '$.id')                        AS category_id,
        JSON_VALUE(cat2, '$.parent_id')                 AS parent_id,
        INITCAP(JSON_VALUE(cat2, '$.name'))             AS category_name,
        CAST(JSON_VALUE(cat2, '$.level') AS INT64)      AS level,
        CAST(JSON_VALUE(cat2, '$.is_active') AS BOOL)   AS is_active,
        CAST(JSON_VALUE(cat2, '$.product_count') AS INT64) AS product_count
    FROM bronze,
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2
),

-- -----------------------------------------------------------------------
-- 3. Aplanar nivel 3 (subcategorías) desde children_data de cada nivel 2
-- -----------------------------------------------------------------------
nivel_3 AS (
    SELECT
        b.bronze_id,
        b.tenant_id,
        b.ingested_at,
        JSON_VALUE(cat3, '$.id')                        AS category_id,
        JSON_VALUE(cat3, '$.parent_id')                 AS parent_id,
        INITCAP(JSON_VALUE(cat3, '$.name'))             AS category_name,
        CAST(JSON_VALUE(cat3, '$.level') AS INT64)      AS level,
        CAST(JSON_VALUE(cat3, '$.is_active') AS BOOL)   AS is_active,
        CAST(JSON_VALUE(cat3, '$.product_count') AS INT64) AS product_count
    FROM bronze AS b,
    UNNEST(JSON_QUERY_ARRAY(b.raw_json, '$.children_data')) AS cat2,
    UNNEST(JSON_QUERY_ARRAY(cat2, '$.children_data'))       AS cat3
),

-- -----------------------------------------------------------------------
-- 4. Unir niveles 2 y 3
-- -----------------------------------------------------------------------
all_levels AS (
    SELECT * FROM nivel_2
    UNION ALL
    SELECT * FROM nivel_3
),

-- -----------------------------------------------------------------------
-- 5. Agregar level_name y deduplicar por tenant + category_id
-- -----------------------------------------------------------------------
deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            CASE level
                WHEN 2 THEN 'categoria'
                WHEN 3 THEN 'subcategoria'
            END                                         AS level_name,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, category_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM all_levels
        WHERE category_id IS NOT NULL
    )
    WHERE rn = 1
)

-- -----------------------------------------------------------------------
-- 6. Output final
-- -----------------------------------------------------------------------
SELECT
    tenant_id,
    category_name,
    category_id,
    parent_id, 
    level_name,
    is_active,
    product_count,
    bronze_id,
    ingested_at
FROM deduped