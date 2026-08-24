-- models/silver/fact_web_events.sql
-- Fuente: GA4 export nativo (analytics_479606051.events_*)
-- Tenant: solo PF
-- Granularidad: una fila por evento
-- PK: hash (GA4 no expone event_id nativo)
-- user_id en GA4 = company_id (decisión de identidad B2B, no customer_id individual)
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'event_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['tenant_id']
    )
}}

WITH events_raw AS (
    SELECT *
    FROM {{ source('ga4_pf', 'events') }}
    WHERE _TABLE_SUFFIX NOT LIKE '%intraday%'
    {% if is_incremental() %}
        AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    {% endif %}
),

parsed AS (
    SELECT
        'pf'                                                    AS tenant_id,

        TO_HEX(SHA256(CONCAT(
            'pf',
            COALESCE(user_pseudo_id, ''),
            CAST(event_timestamp AS STRING),
            event_name,
            COALESCE(CAST({{ ga4_param('batch_event_index', 'int') }} AS STRING), ''),
            COALESCE(CAST({{ ga4_param('batch_page_id', 'int') }} AS STRING), '')
        )))                                                     AS event_id,

        SAFE_CAST(user_id AS INT64)                              AS company_id,
        user_pseudo_id,
        CONCAT(
            COALESCE(user_pseudo_id, 'anon'), '-',
            CAST({{ ga4_param('ga_session_id', 'int') }} AS STRING)
        )                                                        AS session_id,

        event_name,
        PARSE_DATE('%Y%m%d', event_date)                         AS event_date,
        TIMESTAMP_MICROS(event_timestamp)                        AS event_ts,

        {{ ga4_param('page_location', 'string') }}                AS page_location,
        {{ ga4_param('page_title', 'string') }}                   AS page_title,

        device.category                                          AS device_category,
        device.operating_system                                  AS device_os,
        traffic_source.source                                    AS traffic_source,
        traffic_source.medium                                    AS traffic_medium,
        session_traffic_source_last_click.cross_channel_campaign.campaign_name AS utm_campaign,
        SAFE_CAST(
            (SELECT ep.value.double_value
             FROM UNNEST(event_params) AS ep
             WHERE ep.key = 'value'
             LIMIT 1)
        AS FLOAT64)                                                     AS event_value,
        geo.region                                                AS geo_region,
        geo.city                                                  AS geo_city

    FROM events_raw
),

-- -----------------------------------------------------------------------
-- Deduplicar — GA4 puede entregar el mismo evento repetido en el export
-- nativo (reintentos de red del lado cliente, o reprocesamiento previo a
-- la consolidación final de la tabla diaria). Confirmado 2026-07: filas
-- 100% idénticas en todos los campos, mismo event_id. Se conserva 1 copia.
-- -----------------------------------------------------------------------
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts) AS rn
    FROM parsed
)

SELECT
    deduped.tenant_id,
    deduped.event_id,
    deduped.company_id,
    deduped.user_pseudo_id,
    deduped.session_id,
    deduped.event_name,
    deduped.event_date,
    deduped.event_ts,
    deduped.page_location,
    deduped.page_title,
    deduped.device_category,
    deduped.device_os,
    deduped.traffic_source,
    deduped.traffic_medium,
    deduped.utm_campaign,
    deduped.event_value,
    deduped.geo_region,
    deduped.geo_city,

    c.company_id    AS company_source_id,
    c.company_name,
    c.contact_email  AS company_contact_email

FROM deduped
LEFT JOIN {{ ref('dim_company') }} c
    ON deduped.tenant_id = c.tenant_id
    AND deduped.company_id = SAFE_CAST(c.company_id AS INT64)
WHERE deduped.rn = 1
