-- models/gold/category_combinations.sql
-- Fuente: silver.fact_orders + silver.fact_order_items + silver.dim_product
-- Granularidad: top 10 pares de categorías por mes + tenant
{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}
WITH categorias AS (
    SELECT DISTINCT
        o.tenant_id,
        o.order_id,
        o.created_at,
        FORMAT_DATE('%Y-%m', DATE(o.created_at))        AS mes,
        p.category_name
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    LEFT JOIN {{ ref('dim_product') }} p
        ON oi.sku = p.sku AND oi.tenant_id = p.tenant_id
    WHERE p.category_name IS NOT NULL
),
ordenes_mes AS (
    SELECT tenant_id, mes, MIN(created_at) AS created_at, COUNT(DISTINCT order_id) AS total_ordenes
    FROM categorias
    GROUP BY tenant_id, mes
),
combinaciones AS (
    SELECT
        a.tenant_id,
        a.mes,
        a.category_name AS categoria_A,
        b.category_name AS categoria_B,
        COUNT(*)         AS veces_juntas
    FROM categorias a
    JOIN categorias b
        ON a.order_id = b.order_id
        AND a.mes = b.mes
        AND a.tenant_id = b.tenant_id
        AND a.category_name < b.category_name
    GROUP BY a.tenant_id, a.mes, a.category_name, b.category_name
),
ranking AS (
    SELECT
        c.tenant_id,
        c.mes,
        om.created_at,
        c.categoria_A,
        c.categoria_B,
        c.veces_juntas,
        ROUND(c.veces_juntas * 100.0 / om.total_ordenes, 2) AS porcentaje_ordenes,
        ROW_NUMBER() OVER (PARTITION BY c.tenant_id, c.mes ORDER BY c.veces_juntas DESC) AS posicion
    FROM combinaciones c
    JOIN ordenes_mes om ON c.mes = om.mes AND c.tenant_id = om.tenant_id
)
SELECT
    tenant_id, mes, created_at, posicion,
    categoria_A, categoria_B, veces_juntas, porcentaje_ordenes
FROM ranking
WHERE posicion <= 10
