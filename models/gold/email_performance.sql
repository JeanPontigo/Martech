-- models/gold/email_performance.sql
-- Consolidación de campañas de email marketing de todos los tenants
-- Granularidad: un registro por campaign_id
-- Excluye correos de credenciales de PF (utm_campaign LIKE 'credenciales%')
--
-- Fuente: silver.fact_email
-- Tenants: DUOC, AATN, MC (Fidelizador) + PF (Mailup)

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

SELECT
    tenant_id,
    DATE(sent_at)                                               AS sent_date,
    campaign_id,
    utm_campaign,
    email_type,
    campaign_name,
    subject,
    category,
    list_source,
    sender,
    campaign_type,
    sent_count,
    open_count,
    click_count,
    bounced_count,
    unsubscribed_count,
    complaints,
    ROUND(open_count / NULLIF(sent_count, 0) * 100, 2)         AS open_rate,
    ROUND(click_count / NULLIF(sent_count, 0) * 100, 2)        AS click_rate
FROM {{ ref('fact_email') }}
WHERE NOT (tenant_id = 'PF' AND utm_campaign LIKE 'credenciales%')
