-- models/gold/product_combinations.sql
-- Fuente: silver.fact_orders + silver.fact_order_items + silver.dim_product
-- Granularidad: top pares de productos por combinación de subcategorías por mes + tenant
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
        FORMAT_DATE('%Y-%m', DATE(o.created_at_chile))        AS mes,
        MIN(o.created_at_chile)                               AS fecha,
        p.subcategory_name,
        oi.sku,
        p.sku_name
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    LEFT JOIN {{ ref('dim_product') }} p
        ON oi.sku = p.sku AND oi.tenant_id = p.tenant_id
    WHERE p.subcategory_name IS NOT NULL
    AND o.status_norm = 'pagada'
    GROUP BY o.tenant_id, o.order_id, mes, p.subcategory_name, oi.sku, p.sku_name
),
combinaciones AS (
    SELECT
        a.tenant_id,
        a.mes,
        MIN(a.fecha)            AS fecha,
        a.subcategory_name      AS subcategoria_A,
        b.subcategory_name      AS subcategoria_B,
        a.order_id,
        a.sku                   AS sku_A,
        a.sku_name              AS producto_A,
        b.sku                   AS sku_B,
        b.sku_name              AS producto_B
    FROM subcategorias a
    JOIN subcategorias b
        ON a.order_id = b.order_id
        AND a.mes = b.mes
        AND a.tenant_id = b.tenant_id
        AND a.subcategory_name < b.subcategory_name
    GROUP BY a.tenant_id, a.mes, a.subcategory_name, b.subcategory_name, a.order_id, a.sku, a.sku_name, b.sku, b.sku_name
),
ranking_subcat AS (
    SELECT
        tenant_id,
        mes,
        subcategoria_A,
        subcategoria_B,
        COUNT(DISTINCT order_id) AS veces_juntas,
        ROW_NUMBER() OVER (PARTITION BY tenant_id, mes ORDER BY COUNT(DISTINCT order_id) DESC) AS posicion
    FROM combinaciones
    GROUP BY tenant_id, mes, subcategoria_A, subcategoria_B
),
top_subcat AS (
    SELECT tenant_id, mes, subcategoria_A, subcategoria_B, veces_juntas
    FROM ranking_subcat
    WHERE posicion <= 10
),
combinaciones_prod AS (
    SELECT
        ts.tenant_id,
        ts.mes,
        MIN(c.fecha)                                                                  AS fecha,
        CONCAT(ts.subcategoria_A, ' + ', ts.subcategoria_B)                           AS combinacion_subcat,
        ts.veces_juntas                                                               AS ordenes_combinacion,
        CASE WHEN c.sku_A < c.sku_B THEN c.sku_A ELSE c.sku_B END                     AS sku_A,
        CASE WHEN c.sku_A < c.sku_B THEN c.producto_A ELSE c.producto_B END           AS producto_A,
        CASE WHEN c.sku_A < c.sku_B THEN c.sku_B ELSE c.sku_A END                     AS sku_B,
        CASE WHEN c.sku_A < c.sku_B THEN c.producto_B ELSE c.producto_A END           AS producto_B,
        COUNT(DISTINCT c.order_id)                                                    AS veces_juntos
    FROM top_subcat ts
    JOIN combinaciones c
        ON ts.tenant_id = c.tenant_id
        AND ts.mes = c.mes
        AND ts.subcategoria_A = c.subcategoria_A
        AND ts.subcategoria_B = c.subcategoria_B
    GROUP BY ts.tenant_id, ts.mes, combinacion_subcat, ts.veces_juntas, sku_A, producto_A, sku_B, producto_B
)
SELECT
    tenant_id,
    mes,
    fecha,
    combinacion_subcat,
    ordenes_combinacion,
    sku_A,
    producto_A,
    sku_B,
    producto_B,
    veces_juntos,
    ROW_NUMBER() OVER (
        PARTITION BY tenant_id, mes, combinacion_subcat
        ORDER BY veces_juntos DESC
    )                                                                                   AS posicion
FROM combinaciones_prod
