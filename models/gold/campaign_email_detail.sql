-- models/gold/campaign_email_detail.sql
-- Fuente: silver.fact_email + silver.skus_campanas_pf
-- Granularidad: una fila por campaña + utm_campaign
{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}
SELECT
    fe.tenant_id,
    sc.id_campaign,
    sc.nombre_campaign,
    fe.utm_campaign,
    fe.subject,
    fe.sent_at,
    fe.sent_count,
    fe.open_count,
    fe.click_count,
    fe.bounced_count,
    fe.unsubscribed_count
FROM {{ source('silver', 'skus_campanas_pf') }} sc
JOIN {{ ref('fact_email') }} fe
    ON sc.nombre_campaign = fe.campaign_name
    AND fe.tenant_id = 'PF'
WHERE fe.campaign_name IS NOT NULL
