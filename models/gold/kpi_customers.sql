-- models/gold/kpi_customers.sql
-- Fuente: gold.customer_360
-- Granularidad: snapshot diario — una fila por dia + tenant
-- Foto del estado de la base de clientes en cada corrida

{{
    config(
        materialized='incremental',
        unique_key='kpi_id',
        incremental_strategy='merge',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Leer customer_360
-- -----------------------------------------------------------------------
c360 AS (
    SELECT
        client_id,
        tenant_id,
        total_orders,
        total_revenue,
        last_purchase,
        first_purchase,
        rfm_segment
    FROM {{ ref('customer_360') }}
    WHERE total_orders IS NOT NULL
),

-- -----------------------------------------------------------------------
-- 2. Calcular metricas agregadas
-- -----------------------------------------------------------------------
metrics AS (
    SELECT
        tenant_id,
        CURRENT_DATE('UTC')                                                     AS date,

        -- Total clientes con al menos una orden
        COUNT(DISTINCT client_id)                                               AS total_customers,

        -- Clientes activos — ultima compra en ultimos 90 dias
        COUNTIF(
            DATE_DIFF(CURRENT_DATE('UTC'), last_purchase, DAY) <= 90
        )                                                                       AS total_active,

        -- Clientes nuevos — primera compra en ultimos 30 dias
        COUNTIF(
            DATE_DIFF(CURRENT_DATE('UTC'), first_purchase, DAY) <= 30
        )                                                                       AS new_customers,

        -- Clientes recurrentes — mas de 1 orden
        COUNTIF(total_orders > 1)                                               AS returning_customers,

        -- Churn rate — sin compra en 90 dias / total
        ROUND(
            SAFE_DIVIDE(
                COUNTIF(DATE_DIFF(CURRENT_DATE('UTC'), last_purchase, DAY) > 90),
                COUNT(DISTINCT client_id)
            ), 4
        )                                                                       AS churn_rate,

        -- Retention rate — 1 - churn_rate
        ROUND(
            1 - SAFE_DIVIDE(
                COUNTIF(DATE_DIFF(CURRENT_DATE('UTC'), last_purchase, DAY) > 90),
                COUNT(DISTINCT client_id)
            ), 4
        )                                                                       AS retention_rate,

        -- LTV promedio de clientes activos
        ROUND(
            AVG(CASE
                WHEN DATE_DIFF(CURRENT_DATE('UTC'), last_purchase, DAY) <= 90
                THEN total_revenue
            END), 2
        )                                                                       AS avg_ltv

    FROM c360
    GROUP BY tenant_id
),

-- -----------------------------------------------------------------------
-- 3. RFM distribution — conteo por segmento
-- -----------------------------------------------------------------------
rfm_dist AS (
    SELECT
        tenant_id,
        TO_JSON_STRING(
            STRUCT(
                COUNTIF(rfm_segment = 'Champions')  AS Champions,
                COUNTIF(rfm_segment = 'Loyal')      AS Loyal,
                COUNTIF(rfm_segment = 'At Risk')    AS At_Risk,
                COUNTIF(rfm_segment = 'Lost')       AS Lost,
                COUNTIF(rfm_segment = 'New')        AS New_customers
            )
        )                                                                       AS rfm_distribution
    FROM c360
    GROUP BY tenant_id
)

-- -----------------------------------------------------------------------
-- 4. Output final
-- -----------------------------------------------------------------------
SELECT
    TO_HEX(SHA256(CONCAT(
        m.tenant_id,
        CAST(m.date AS STRING)
    )))                                                                         AS kpi_id,
    m.tenant_id,
    m.date,
    m.total_customers,
    m.total_active,
    m.new_customers,
    m.returning_customers,
    m.churn_rate,
    m.retention_rate,
    m.avg_ltv,
    rd.rfm_distribution,
    CURRENT_DATETIME('UTC')                                                     AS last_updated
FROM metrics m
LEFT JOIN rfm_dist rd ON m.tenant_id = rd.tenant_id
