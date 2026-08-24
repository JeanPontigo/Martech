-- models/gold/segments.sql
-- Fuente: gold.customer_360
-- Granularidad: una fila por segmento por tenant
-- Tipos: rfm (activo) / custom (pendiente definicion en equipo)

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Contar clientes por segmento RFM desde customer_360
-- -----------------------------------------------------------------------
rfm_segments AS (
    SELECT
        tenant_id,
        rfm_segment                                                             AS segment_name,
        COUNT(DISTINCT client_id)                                               AS customer_count
    FROM {{ ref('customer_360') }}
    WHERE rfm_segment IS NOT NULL
    GROUP BY tenant_id, rfm_segment
),

-- -----------------------------------------------------------------------
-- 2. Agregar descripcion y metadata por segmento
-- -----------------------------------------------------------------------
enriched AS (
    SELECT
        tenant_id,
        segment_name,
        customer_count,
        'rfm'                                                                   AS type,
        CASE segment_name
            WHEN 'Champions'  THEN 'Clientes que compran frecuente, reciente y gastan mucho. Prioridad VIP.'
            WHEN 'Loyal'      THEN 'Clientes frecuentes con buen gasto. Alta probabilidad de retención.'
            WHEN 'At Risk'    THEN 'Antes buenos clientes, hace tiempo que no compran. Requieren reactivación.'
            WHEN 'Lost'       THEN 'Clientes inactivos con baja frecuencia y gasto. Difíciles de recuperar.'
            WHEN 'New'        THEN 'Compraron recientemente por primera vez. Requieren onboarding y fidelización.'
            ELSE 'Sin descripción'
        END                                                                     AS description
    FROM rfm_segments
)

-- -----------------------------------------------------------------------
-- 3. Output final
-- -----------------------------------------------------------------------
SELECT
    TO_HEX(SHA256(CONCAT(
        tenant_id,
        type,
        segment_name
    )))                                                                         AS segment_id,
    tenant_id,
    segment_name,
    type,
    customer_count,
    description,
    CURRENT_DATETIME('UTC')                                                     AS last_updated
FROM enriched
