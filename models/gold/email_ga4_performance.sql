-- models/gold/email_ga4_performance.sql
-- Une email_performance con datos de GA4 de PF
-- JOIN por utm_campaign = traffic_source.name
-- GA4 agregado por campaña antes del JOIN para evitar multiplicación de filas
--
-- Tenants GA4 disponibles: PF (analytics_479606051)
-- Datos GA4 disponibles desde: 2026-07-24

{{ config(materialized='table') }}

WITH

-- -----------------------------------------------------------------------
-- 1. Agregar GA4 por campaña — una fila por utm_campaign
-- -----------------------------------------------------------------------
ga4_pf AS (
    SELECT
        traffic_source.name                         AS utm_campaign,
        COUNTIF(event_name = 'session_start')       AS sessions,
        COUNTIF(event_name = 'purchase')            AS transactions,
        SUM(
            CASE WHEN event_name = 'purchase'
            THEN event_value_in_usd ELSE 0 END
        )                                           AS total_revenue
    FROM `martech-data-platform-atlas.analytics_479606051.events_*`
    WHERE traffic_source.name IS NOT NULL
    GROUP BY utm_campaign
),

-- -----------------------------------------------------------------------
-- 2. Unir email_performance con GA4
-- -----------------------------------------------------------------------
email_ga4 AS (
    SELECT
        e.tenant_id,
        e.sent_date,
        e.campaign_id,
        e.utm_campaign,
        e.email_type,
        e.campaign_name,
        e.subject,
        e.category,
        e.list_source,
        e.sender,
        e.campaign_type,
        e.sent_count,
        e.open_count,
        e.click_count,
        e.bounced_count,
        e.unsubscribed_count,
        e.complaints,
        COALESCE(g.sessions, 0)                     AS sessions,
        COALESCE(g.transactions, 0)                 AS transactions,
        COALESCE(g.total_revenue, 0)                AS total_revenue
    FROM {{ ref('email_performance') }} e
    LEFT JOIN ga4_pf g
        ON e.utm_campaign = g.utm_campaign
)

SELECT * FROM email_ga4
