-- models/gold/web_sessions_daily.sql
-- Fuente: silver.fact_web_events
-- Granularidad: una fila por sesión + fecha
-- Permite agregar sesiones por período dinámicamente en Looker Studio

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

SELECT
    tenant_id,
    event_date                                          AS fecha,
    traffic_source,
    traffic_medium,
    utm_campaign,
    device_category,
    geo_region,
    geo_city,
    COUNT(DISTINCT session_id)                          AS sesiones,
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' 
        THEN session_id END)                            AS transacciones,
    SUM(CASE WHEN event_name = 'purchase' 
        THEN event_value ELSE 0 END)                    AS ingresos_ga4,
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN session_id END),
        COUNT(DISTINCT session_id)
    )                                                   AS cvr
FROM {{ ref('fact_web_events') }}
GROUP BY tenant_id, event_date, traffic_source, 
         traffic_medium, utm_campaign, device_category,
         geo_region, geo_city
