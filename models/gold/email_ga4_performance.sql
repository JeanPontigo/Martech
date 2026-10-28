-- models/gold/email_ga4_performance.sql
-- Une todas las sesiones de GA4 con email_performance cuando el UTM coincide
-- GA4 es la tabla principal — todas las sesiones aparecen
-- email_performance se une como LEFT JOIN por utm_campaign
--
-- Tenants GA4 disponibles: PF (analytics_479606051)
-- Datos GA4 disponibles desde: 2026-07-24

{{ config(materialized='table') }}

WITH

-- -----------------------------------------------------------------------
-- 1. Agregar GA4 por campaña y fecha — una fila por utm_campaign + fecha
-- -----------------------------------------------------------------------
ga4_agg AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date)            AS event_date,
        traffic_source.name                         AS utm_campaign,
        traffic_source.medium                       AS medium,
        COUNTIF(event_name = 'session_start')       AS sessions,
        COUNTIF(event_name = 'purchase')            AS transactions,
        ROUND(SUM(
            CASE WHEN event_name = 'purchase'
            THEN event_value_in_usd ELSE 0 END
        ), 2)                                       AS total_revenue,
        ROUND(
            COUNTIF(event_name = 'session_start' AND 
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged') IS NULL
            ) / NULLIF(COUNTIF(event_name = 'session_start'), 0) * 100, 2
        ) AS bounce_rate
    FROM `martech-data-platform-atlas.analytics_479606051.events_*`
    GROUP BY event_date, utm_campaign, medium
),
    
-- -----------------------------------------------------------------------
-- 2. Agregar email_performance por utm_campaign
-- (puede haber múltiples envíos del mismo UTM en distintas fechas)
-- -----------------------------------------------------------------------
email_agg AS (
    SELECT
        utm_campaign,
        MAX(tenant_id)                              AS tenant_id,
        MAX(campaign_id)                            AS campaign_id,
        MAX(campaign_name)                          AS campaign_name,
        MAX(subject)                                AS subject,
        MAX(email_type)                             AS email_type,
        MAX(sent_date)                              AS sent_date,
        SUM(sent_count)                             AS sent_count,
        SUM(open_count)                             AS open_count,
        SUM(click_count)                            AS click_count,
        SUM(bounced_count)                          AS bounced_count,
        SUM(unsubscribed_count)                     AS unsubscribed_count,
        ROUND(SUM(open_count) / NULLIF(SUM(sent_count), 0) * 100, 2) AS open_rate,
        ROUND(SUM(click_count) / NULLIF(SUM(sent_count), 0) * 100, 2) AS click_rate
    FROM {{ ref('email_performance') }}
    GROUP BY utm_campaign
)

-- -----------------------------------------------------------------------
-- 3. JOIN — GA4 como tabla principal
-- -----------------------------------------------------------------------
SELECT
    g.event_date,
    g.utm_campaign,
    g.medium,
    g.sessions,
    g.transactions,
    g.total_revenue,
    g.bounce_rate, 
    e.tenant_id,
    e.campaign_id,
    e.campaign_name,
    e.subject,
    e.email_type,
    e.sent_date,
    e.sent_count,
    e.open_count,
    e.click_count,
    e.bounced_count,
    e.unsubscribed_count,
    e.open_rate,
    e.click_rate
FROM ga4_agg g
LEFT JOIN email_agg e
    ON LOWER(g.utm_campaign) = LOWER(e.utm_campaign)
