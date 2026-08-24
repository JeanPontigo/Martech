-- models/staging/pf/stg_pf__products.sql
-- Normaliza productos PF desde bronze.ecommerce (entity='products')
-- Incluye resolución del árbol de categorías (levels 2-4) + category_ids del producto
-- Materialización: view — solo parseo/JOIN de categorías, sin dedupe
{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id          AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'products'
      AND tenant_id = 'pf'
),
extracted AS (
    SELECT
        bronze_id,
        tenant_id,
        ingested_at,
        JSON_VALUE(raw_json, '$.sku')                                   AS sku,
        INITCAP(JSON_VALUE(raw_json, '$.name'))                         AS sku_name,
        JSON_VALUE(raw_json, '$.type_id')                               AS product_type,
        CASE JSON_VALUE(raw_json, '$.status')
            WHEN '1' THEN 'enabled'
            WHEN '2' THEN 'disabled'
            ELSE 'unknown'
        END                                                             AS catalog_status,
        DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))       AS created_at,
        DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))       AS updated_at,
        (
            SELECT JSON_VALUE(attr, '$.value')
            FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.custom_attributes')) AS attr
            WHERE JSON_VALUE(attr, '$.attribute_code') = 'marca'
            LIMIT 1
        )                                                               AS brand
    FROM bronze
),
-- -----------------------------------------------------------------------
-- Árbol de categorías — niveles 2, 3 y 4
-- -----------------------------------------------------------------------
categories_bronze AS (
    SELECT tenant_id, raw_json
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'categories'
      AND tenant_id = 'pf'
),
nivel_2 AS (
    SELECT
        tenant_id,
        JSON_VALUE(cat2, '$.id')                  AS node_id,
        JSON_VALUE(cat2, '$.parent_id')            AS node_parent_id,
        INITCAP(JSON_VALUE(cat2, '$.name'))        AS node_name,
        CAST(JSON_VALUE(cat2, '$.level') AS INT64) AS node_level
    FROM categories_bronze,
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2
),
nivel_3 AS (
    SELECT
        tenant_id,
        JSON_VALUE(cat3, '$.id')                  AS node_id,
        JSON_VALUE(cat3, '$.parent_id')            AS node_parent_id,
        INITCAP(JSON_VALUE(cat3, '$.name'))        AS node_name,
        CAST(JSON_VALUE(cat3, '$.level') AS INT64) AS node_level
    FROM categories_bronze,
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2,
    UNNEST(JSON_QUERY_ARRAY(cat2, '$.children_data'))     AS cat3
),
nivel_4 AS (
    SELECT
        tenant_id,
        JSON_VALUE(cat4, '$.id')                  AS node_id,
        JSON_VALUE(cat4, '$.parent_id')            AS node_parent_id,
        INITCAP(JSON_VALUE(cat4, '$.name'))        AS node_name,
        CAST(JSON_VALUE(cat4, '$.level') AS INT64) AS node_level
    FROM categories_bronze,
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.children_data')) AS cat2,
    UNNEST(JSON_QUERY_ARRAY(cat2, '$.children_data'))     AS cat3,
    UNNEST(JSON_QUERY_ARRAY(cat3, '$.children_data'))     AS cat4
),
all_nodes AS (
    SELECT * FROM nivel_2
    UNION ALL SELECT * FROM nivel_3
    UNION ALL SELECT * FROM nivel_4
),
product_categories_raw AS (
    SELECT
        b.tenant_id,
        JSON_VALUE(b.raw_json, '$.sku') AS sku,
        JSON_VALUE(cat_id)              AS category_id_raw,
        `offset`                        AS array_position
    FROM {{ source('bronze', 'ecommerce') }} AS b,
    UNNEST(
        JSON_QUERY_ARRAY((
            SELECT JSON_QUERY(attr, '$.value')
            FROM UNNEST(JSON_QUERY_ARRAY(b.raw_json, '$.custom_attributes')) AS attr
            WHERE JSON_VALUE(attr, '$.attribute_code') = 'category_ids'
            LIMIT 1
        ))
    ) AS cat_id WITH OFFSET
    WHERE b.entity = 'products'
      AND b.tenant_id = 'pf'
      AND JSON_VALUE(b.raw_json, '$.sku') IS NOT NULL
),
remapped AS (
    SELECT
        pc.tenant_id,
        pc.sku,
        pc.array_position,
        CASE
            WHEN n.node_level = 4 THEN n.node_parent_id
            ELSE pc.category_id_raw
        END AS resolved_category_id
    FROM product_categories_raw AS pc
    LEFT JOIN all_nodes AS n
        ON pc.tenant_id = n.tenant_id AND pc.category_id_raw = n.node_id
),
resolved AS (
    SELECT
        r.tenant_id,
        r.sku,
        r.array_position,
        n.node_id,
        n.node_level,
        n.node_name,
        n.node_parent_id
    FROM remapped AS r
    LEFT JOIN all_nodes AS n
        ON r.tenant_id = n.tenant_id AND r.resolved_category_id = n.node_id
    WHERE n.node_id IS NOT NULL
),
subcategoria_elegida AS (
    SELECT
        tenant_id,
        sku,
        node_name        AS subcategory_name,
        node_parent_id
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, sku
                ORDER BY array_position ASC
            ) AS rn
        FROM resolved
        WHERE node_level = 3
    )
    WHERE rn = 1
),
categoria_directa AS (
    SELECT
        tenant_id,
        sku,
        node_name AS category_name
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, sku
                ORDER BY array_position ASC
            ) AS rn
        FROM resolved
        WHERE node_level = 2
    )
    WHERE rn = 1
),
categoria_desde_sub AS (
    SELECT
        s.tenant_id,
        s.sku,
        n2.node_name AS category_name,
        s.subcategory_name
    FROM subcategoria_elegida AS s
    LEFT JOIN all_nodes AS n2
        ON s.tenant_id = n2.tenant_id
        AND s.node_parent_id = n2.node_id
        AND n2.node_level = 2
),
category_tree AS (
    SELECT tenant_id, sku, category_name, subcategory_name
    FROM categoria_desde_sub
    UNION ALL
    SELECT cd.tenant_id, cd.sku, cd.category_name, NULL AS subcategory_name
    FROM categoria_directa AS cd
    WHERE NOT EXISTS (
        SELECT 1 FROM categoria_desde_sub cds
        WHERE cds.tenant_id = cd.tenant_id AND cds.sku = cd.sku
    )
)
-- -----------------------------------------------------------------------
-- Output final
-- -----------------------------------------------------------------------
SELECT
    e.bronze_id,
    e.tenant_id,
    e.ingested_at,
    e.sku,
    e.sku_name,
    e.product_type,
    e.catalog_status,
    e.brand,
    cat.category_name,
    cat.subcategory_name,
    e.created_at,
    e.updated_at
FROM extracted AS e
LEFT JOIN category_tree AS cat
    ON e.tenant_id = cat.tenant_id AND e.sku = cat.sku
WHERE e.sku IS NOT NULL
