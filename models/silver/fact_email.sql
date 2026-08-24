-- models/silver/fact_email_pf.sql
-- Fuente: bronze.email_mailup WHERE tenant_id = 'PF'
-- Tenant: PF (Tienda PFalimentos) — Mailup
-- Granularidad: una fila por mensaje (campaign_id = idMessage)
-- PK: tenant_id + campaign_id
--
-- Campos protegidos (nunca se sobrescriben):
--   - campaign_name → llenado manual
--
-- sent_at: extraído de notes con regex DD/MM/YYYY
-- email_type: 'manual' o 'automated'

{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'campaign_id'],
        incremental_strategy='merge',
        merge_update_columns=[
            'subject', 'email_type',
            'sent_count', 'open_count', 'click_count',
            'bounced_count', 'unsubscribed_count',
            'sent_at', 'public_url', 'utm_campaign',
            'last_updated_at'
        ],
        partition_by={
            'field': 'sent_at',
            'data_type': 'timestamp',
            'granularity': 'month'
        },
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Leer Bronze
-- -----------------------------------------------------------------------
bronze AS (
    SELECT
        id,
        tenant_id,
        campaign_id,
        subject,
        notes,
        utm_campaign,
        public_url,
        sent_count,
        open_count,
        click_count,
        bounced_count,
        unsubscribed_count,
        email_type,
        ingested_at
    FROM {{ source('bronze', 'email_mailup') }}
    WHERE tenant_id = 'PF'
    {% if is_incremental() %}
        AND ingested_at > (SELECT MAX(last_updated_at) FROM {{ this }} WHERE tenant_id = 'PF')
    {% endif %}
),

-- -----------------------------------------------------------------------
-- 2. Deduplicar por campaign_id — quedarse con el más reciente
-- -----------------------------------------------------------------------
deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, campaign_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM bronze
    )
    WHERE rn = 1
),

-- -----------------------------------------------------------------------
-- 3. Extraer sent_at desde notes con regex DD/MM/YYYY
-- -----------------------------------------------------------------------
with_dates AS (
    SELECT
        tenant_id,
        campaign_id,
        subject,
        email_type,
        sent_count,
        open_count,
        click_count,
        bounced_count,
        unsubscribed_count,
        public_url,
        utm_campaign,
        ingested_at,

        CASE
            -- 1. Fecha desde notes DD/MM/YYYY
            WHEN REGEXP_CONTAINS(notes, r'\d{2}/\d{2}/\d{4}')
            THEN PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(notes, r'(\d{2}/\d{2}/\d{4})'))

            -- 2. Fecha desde utm_campaign DD/MM/YYYY
            WHEN REGEXP_CONTAINS(utm_campaign, r'\d{2}/\d{2}/\d{4}')
            THEN PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(utm_campaign, r'(\d{2}/\d{2}/\d{4})'))

            -- 3. Fecha desde utm_campaign DD-MM-YYYY
            WHEN REGEXP_CONTAINS(utm_campaign, r'\d{2}-\d{2}-\d{4}')
            THEN PARSE_DATE('%d-%m-%Y', REGEXP_EXTRACT(utm_campaign, r'(\d{2}-\d{2}-\d{4})'))

            ELSE NULL
        END AS sent_at

    FROM deduped
)

-- -----------------------------------------------------------------------
-- 4. Output final — mapeo a silver.fact_email
-- -----------------------------------------------------------------------
SELECT
    tenant_id,
    campaign_id,
    CAST(NULL AS STRING)            AS campaign_name,
    subject,
    email_type,
    sent_count,
    open_count,
    click_count,
    bounced_count,
    unsubscribed_count,
    CAST(NULL AS INT64)             AS complaints,
    CAST(sent_at AS TIMESTAMP)      AS sent_at,
    public_url,
    utm_campaign,
    CAST(NULL AS STRING)            AS sender,
    CAST(NULL AS STRING)            AS campaign_type,
    CAST(NULL AS STRING)            AS category,
    CAST(NULL AS STRING)            AS list_source,
    CAST(NULL AS STRING)            AS base_type,
    CURRENT_TIMESTAMP()             AS last_updated_at
FROM with_dates
