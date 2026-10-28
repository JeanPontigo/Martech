-- models/gold/category_combinations.sql
-- Fuente: silver.fact_orders + silver.fact_order_items + silver.dim_product
-- Granularidad: top 10 pares de subcategorías por mes + tenant
{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}
WITH subcategorias AS (
    SELECT DISTINCT
        o.tenant_id,
        o.order_id,
        o.created_at_chile,
        FORMAT_DATE('%Y-%m', DATE(o.created_at_chile))        AS mes,
        p.subcategory_name
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    LEFT JOIN {{ ref('dim_product') }} p
        ON oi.sku = p.sku AND oi.tenant_id = p.tenant_id
    WHERE p.subcategory_name IS NOT NULL
    AND o.status_norm = 'pagada'
),
ordenes_mes AS (
    SELECT tenant_id, mes, MIN(created_at_chile) AS created_at, COUNT(DISTINCT order_id) AS total_ordenes
    FROM subcategorias
    GROUP BY tenant_id, mes
),
combinaciones AS (
    SELECT
        a.tenant_id,
        a.mes,
        a.subcategory_name AS subcategoria_A,
        b.subcategory_name AS subcategoria_B,
        COUNT(*)           AS veces_juntas
    FROM subcategorias a
    JOIN subcategorias b
        ON a.order_id = b.order_id
        AND a.mes = b.mes
        AND a.tenant_id = b.tenant_id
        AND a.subcategory_name < b.subcategory_name
    GROUP BY a.tenant_id, a.mes, a.subcategory_name, b.subcategory_name
),
ranking AS (
    SELECT
        c.tenant_id,
        c.mes,
        CONCAT(subcategoria_A, ' + ', subcategoria_B) AS combinacion_subcat,
        om.created_at,
        DATE(CONCAT(c.mes, '-01')) AS fecha,
        c.subcategoria_A,
        c.subcategoria_B,
        c.veces_juntas,
        ROUND(c.veces_juntas * 100.0 / om.total_ordenes, 2) AS porcentaje_ordenes,
        ROW_NUMBER() OVER (PARTITION BY c.tenant_id, c.mes ORDER BY c.veces_juntas DESC) AS posicion
    FROM combinaciones c
    JOIN ordenes_mes om ON c.mes = om.mes AND c.tenant_id = om.tenant_id
)
SELECT
    tenant_id,
    mes,
    created_at,
    posicion,
    fecha,
    subcategoria_A,
    subcategoria_B,
    combinacion_subcat,
    veces_juntas,
    porcentaje_ordenes
FROM ranking
WHERE posicion <= 10
