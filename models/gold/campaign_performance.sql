-- models/gold/campaign_performance.sql
-- Fuente: silver.skus_campanas_pf + silver.fact_orders + silver.fact_order_items
--         + silver.dim_product + silver.dim_product_category + silver.dim_category
--         + silver.fact_email + silver.fact_web_events
-- Granularidad: una fila por campaña + SKU + fecha
-- Incluye métricas transaccionales (durante/previo) y email 

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Campañas con período de comparación calculado
-- -----------------------------------------------------------------------
campanas AS (
    SELECT
        id_campaign,
        nombre_campaign,
        sku,
        fecha_inicio,
        fecha_termino,
        estado,
        DATE_DIFF(fecha_termino, fecha_inicio, DAY) + 1          AS duracion_campana,
        DATE_SUB(fecha_inicio, INTERVAL DATE_DIFF(fecha_termino, fecha_inicio, DAY) + 1 DAY) AS periodo_comparacion
    FROM {{ source('silver', 'skus_campanas_pf') }}
),

-- -----------------------------------------------------------------------
-- 2. Órdenes válidas con sus items
-- -----------------------------------------------------------------------
ordenes_items AS (
    SELECT
        o.order_id,
        o.client_id,
        DATE(o.created_at)      AS fecha_venta,
        o.tenant_id,
        oi.sku,
        oi.line_total_net        AS sku_total_value,
        oi.qty_ordered           AS sku_qty
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi
        ON o.order_id = oi.order_id AND o.tenant_id = oi.tenant_id
    WHERE o.tenant_id = 'pf'
    AND o.status IN ('completado', 'facturado', 'en_reparto')
),

-- -----------------------------------------------------------------------
-- 3. JOIN campañas con órdenes — solo el rango relevante
-- -----------------------------------------------------------------------
joined AS (
    SELECT
        c.id_campaign,
        c.nombre_campaign,
        c.fecha_inicio,
        c.fecha_termino,
        c.duracion_campana,
        c.periodo_comparacion,
        c.estado,
        c.sku,
        oi.order_id,
        oi.client_id,
        oi.tenant_id,
        oi.fecha_venta,
        oi.sku_total_value,
        oi.sku_qty
    FROM campanas c
    JOIN ordenes_items oi ON c.sku = oi.sku
    WHERE oi.fecha_venta BETWEEN c.periodo_comparacion AND c.fecha_termino
),

-- -----------------------------------------------------------------------
-- 4. Metadata de producto
-- -----------------------------------------------------------------------
productos AS (
    SELECT
        sku,
        sku_name,
        brand
    FROM {{ ref('dim_product') }}
    WHERE tenant_id = 'pf'
),

-- -----------------------------------------------------------------------
-- 5. Categoría principal del producto
-- -----------------------------------------------------------------------
categorias AS (
    SELECT
        sku,
        category_name
    FROM {{ ref('dim_product') }}
    WHERE tenant_id = 'pf'
),

-- -----------------------------------------------------------------------
-- 6. Métricas de email por campaña — incluye utm_campaign para JOIN con GA4
-- -----------------------------------------------------------------------
email_metrics AS (
    SELECT
        campaign_name,
        STRING_AGG(DISTINCT utm_campaign, ', ') AS utm_campaigns,
        SUM(sent_count)           AS sent_count,
        SUM(open_count)           AS open_count,
        SUM(click_count)          AS click_count,
        SUM(bounced_count)        AS bounced_count,
        SUM(unsubscribed_count)   AS unsubscribed_count,
        MIN(sent_at)              AS first_sent_at,
        MAX(sent_at)              AS last_sent_at
    FROM {{ ref('fact_email') }}
    WHERE tenant_id = 'PF'
    AND campaign_name IS NOT NULL
    GROUP BY campaign_name
)

-- -----------------------------------------------------------------------
-- 7. Output final — granularidad campaña + SKU + fecha
-- -----------------------------------------------------------------------
SELECT
    j.id_campaign,
    j.nombre_campaign,
    j.fecha_inicio,
    j.fecha_termino,
    j.duracion_campana,
    j.periodo_comparacion,
    j.estado,
    j.sku,
    p.sku_name,
    p.brand,
    cat.category_name,
    j.fecha_venta,
    j.tenant_id,
    CASE
        WHEN j.fecha_venta BETWEEN j.fecha_inicio AND j.fecha_termino THEN 'durante'
        ELSE 'previo'
    END                                                         AS periodo,
    j.sku_total_value                                           AS venta,
    j.sku_qty                                                   AS unidades,
    j.order_id,
    j.client_id,
    CASE WHEN j.fecha_venta BETWEEN j.fecha_inicio AND j.fecha_termino
        THEN j.sku_total_value ELSE 0 END                       AS venta_durante,
    CASE WHEN j.fecha_venta BETWEEN j.fecha_inicio AND j.fecha_termino
        THEN j.sku_qty ELSE 0 END                               AS unidades_durante,
    CASE WHEN j.fecha_venta BETWEEN j.periodo_comparacion AND DATE_SUB(j.fecha_inicio, INTERVAL 1 DAY)
        THEN j.sku_total_value ELSE 0 END                       AS venta_previo,
    CASE WHEN j.fecha_venta BETWEEN j.periodo_comparacion AND DATE_SUB(j.fecha_inicio, INTERVAL 1 DAY)
        THEN j.sku_qty ELSE 0 END                               AS unidades_previo,
    em.sent_count,
    em.open_count,
    em.click_count,
    em.bounced_count,
    em.unsubscribed_count,
    em.first_sent_at,
    em.last_sent_at,
    em.utm_campaigns
FROM joined j
LEFT JOIN productos p ON j.sku = p.sku
LEFT JOIN categorias cat ON j.sku = cat.sku
LEFT JOIN email_metrics em ON j.nombre_campaign = em.campaign_name
