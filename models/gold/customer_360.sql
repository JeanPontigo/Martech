-- models/gold/customer_360.sql
-- Fuente: silver.dim_client + silver.fact_orders + silver.fact_order_items
--         + silver.dim_company + silver.fact_web_events (PF)
-- Granularidad: una fila por cliente
-- RFM calculado con NTILE(5) sobre las 3 dimensiones
-- Campos NULL: cltv, churn_probability (BigQuery ML)
--              email_* (Fidelizador/Mailup pendiente)

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Base de clientes desde dim_client
-- -----------------------------------------------------------------------
clients AS (
    SELECT
        client_id,
        tenant_id,
        email,
        email_clean,
        first_name,
        last_name,
        rut,
        group_id,
        group_name,
        company_id
    FROM {{ ref('dim_client') }}
),
    
-- -----------------------------------------------------------------------
-- 2. Información de empresa desde dim_company
-- -----------------------------------------------------------------------
company_info AS (
    SELECT
        tenant_id,
        company_id,
        company_name
    FROM {{ ref('dim_company') }}
),

-- -----------------------------------------------------------------------
-- 3. Métricas web desde fact_web_events (granularidad: company_id)
-- -----------------------------------------------------------------------
web_metrics AS (
    SELECT
        tenant_id,
        CAST(company_id AS STRING)                                  AS company_id,
        COUNT(DISTINCT session_id)                                  AS sessions_total,
        COUNT(DISTINCT CASE
            WHEN event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            THEN session_id
        END)                                                        AS sessions_last_30d,
        MAX(event_date)                                             AS last_session,
        ROUND(SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN session_id END),
            COUNT(DISTINCT session_id)
        ), 4)                                                       AS conversion_rate
    FROM {{ ref('fact_web_events') }}
    WHERE company_id IS NOT NULL
    GROUP BY tenant_id, company_id
),
    
-- -----------------------------------------------------------------------
-- 4. Metricas transaccionales desde fact_orders
-- -----------------------------------------------------------------------
order_metrics AS (
    SELECT
        client_id,
        tenant_id,
        COUNT(DISTINCT order_id)                AS total_orders,
        SUM(subtotal_net)                       AS total_revenue,
        ROUND(SAFE_DIVIDE(
            SUM(subtotal_net),
            COUNT(DISTINCT order_id)
        ), 2)                                   AS avg_ticket,
        MIN(DATE(created_at))                   AS first_purchase,
        MAX(DATE(created_at))                   AS last_purchase,
        DATE_DIFF(
            CURRENT_DATE('UTC'),
            MAX(DATE(created_at)),
            DAY
        )                                       AS days_since_last_purchase,
        -- Medio de pago mas frecuente
        APPROX_TOP_COUNT(medio_pago, 1)[OFFSET(0)].value AS preferred_payment_method
    FROM {{ ref('fact_orders') }}
    GROUP BY client_id, tenant_id
),

-- -----------------------------------------------------------------------
-- 5. Top 3 SKUs por cliente
-- -----------------------------------------------------------------------
sku_ranked AS (
    SELECT
        o.client_id,
        o.tenant_id,
        oi.sku,
        SUM(oi.line_total_net)  AS sku_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY o.client_id, o.tenant_id
            ORDER BY SUM(oi.line_total_net) DESC
        )                       AS sku_rank
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi ON o.order_id = oi.order_id
    GROUP BY o.client_id, o.tenant_id, oi.sku
),

top_skus AS (
    SELECT
        client_id,
        tenant_id,
        TO_JSON_STRING(
            ARRAY_AGG(STRUCT(sku, sku_revenue) ORDER BY sku_rank)
        ) AS top_skus
    FROM sku_ranked
    WHERE sku_rank <= 3
    GROUP BY client_id, tenant_id
),

-- -----------------------------------------------------------------------
-- 6. Top 3 categorias por cliente
-- -----------------------------------------------------------------------
category_ranked AS (
    SELECT
        o.client_id,
        o.tenant_id,
        pc.category_id,
        SUM(oi.line_total_net)  AS cat_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY o.client_id, o.tenant_id
            ORDER BY SUM(oi.line_total_net) DESC
        )                       AS cat_rank
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('dim_product_category') }} pc
        ON  oi.sku       = pc.sku
        AND oi.tenant_id = pc.tenant_id
    GROUP BY o.client_id, o.tenant_id, pc.category_id
),

top_categories AS (
    SELECT
        client_id,
        tenant_id,
        TO_JSON_STRING(
            ARRAY_AGG(STRUCT(category_id, cat_revenue) ORDER BY cat_rank)
        ) AS top_categories
    FROM category_ranked
    WHERE cat_rank <= 3
    GROUP BY client_id, tenant_id
),

-- -----------------------------------------------------------------------
-- 7. RFM — scores con NTILE(5) sobre las 3 dimensiones
-- -----------------------------------------------------------------------
rfm_base AS (
    SELECT
        client_id,
        tenant_id,
        days_since_last_purchase,
        total_orders,
        total_revenue
    FROM order_metrics
),

rfm_scores AS (
    SELECT
        client_id,
        tenant_id,
        -- Recency: menos dias = mejor (invertido)
        6 - NTILE(5) OVER (
            PARTITION BY tenant_id
            ORDER BY days_since_last_purchase DESC
        )                   AS rfm_r,
        -- Frequency: mas ordenes = mejor
        NTILE(5) OVER (
            PARTITION BY tenant_id
            ORDER BY total_orders ASC
        )                   AS rfm_f,
        -- Monetary: mas revenue = mejor
        NTILE(5) OVER (
            PARTITION BY tenant_id
            ORDER BY total_revenue ASC
        )                   AS rfm_m
    FROM rfm_base
),

rfm_final AS (
    SELECT
        client_id,
        tenant_id,
        rfm_r,
        rfm_f,
        rfm_m,
        rfm_r * 100 + rfm_f * 10 + rfm_m   AS rfm_score,
        CASE
            WHEN rfm_r >= 4 AND rfm_f >= 4 AND rfm_m >= 4 THEN 'Champions'
            WHEN rfm_r >= 3 AND rfm_f >= 3 AND rfm_m >= 3 THEN 'Loyal'
            WHEN rfm_r >= 3 AND rfm_f <= 2                THEN 'New'
            WHEN rfm_r <= 2 AND rfm_f >= 3                THEN 'At Risk'
            ELSE                                                'Lost'
        END                                 AS rfm_segment
    FROM rfm_scores
),

-- -----------------------------------------------------------------------
-- 8. Ensamble final
-- -----------------------------------------------------------------------
assembled AS (
    SELECT
        -- Identidad
        c.client_id,
        c.tenant_id,
        c.email,
        c.first_name,
        c.last_name,
        c.rut,
        c.group_name,
        co.company_name,

        -- Transaccional
        om.first_purchase,
        om.last_purchase,
        om.days_since_last_purchase,
        om.total_orders,
        om.total_revenue,
        om.avg_ticket,
        om.preferred_payment_method,
        ts.top_skus,
        tc.top_categories,

        -- Comportamiento web
        wm.sessions_last_30d,
        wm.last_session,
        wm.conversion_rate,

        -- Email — pendiente Fidelizador/Mailup
        NULL AS email_subscribed,
        NULL AS email_open_rate,
        NULL AS email_click_rate,
        NULL AS email_ctor,
        NULL AS last_email_open,

        -- Scores y segmentos
        rf.rfm_r,
        rf.rfm_f,
        rf.rfm_m,
        rf.rfm_score,
        rf.rfm_segment,
        NULL AS cltv,
        NULL AS churn_probability,
        NULL AS custom_segments,
        NULL AS next_best_buy,

        -- Metadata
        CURRENT_DATETIME('UTC') AS last_updated

    FROM clients c
    LEFT JOIN company_info co       ON SAFE_CAST(c.company_id AS STRING) = co.company_id
                                   AND c.tenant_id = co.tenant_id
    LEFT JOIN order_metrics om      ON c.client_id  = om.client_id
                                   AND c.tenant_id  = om.tenant_id
    LEFT JOIN top_skus ts           ON c.client_id  = ts.client_id
                                   AND c.tenant_id  = ts.tenant_id
    LEFT JOIN top_categories tc     ON c.client_id  = tc.client_id
                                   AND c.tenant_id  = tc.tenant_id
    LEFT JOIN rfm_final rf          ON c.client_id  = rf.client_id
                                   AND c.tenant_id  = rf.tenant_id
    LEFT JOIN web_metrics wm        ON c.company_id = wm.company_id
                                   AND c.tenant_id  = wm.tenant_id
)

-- -----------------------------------------------------------------------
-- 9. Output final
-- -----------------------------------------------------------------------
SELECT *
FROM assembled
