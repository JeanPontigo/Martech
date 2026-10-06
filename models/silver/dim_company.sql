-- models/silver/dim_company.sql
-- Fuente: staging normalizado por tenant (ver models/staging/)
-- Tenant: pf, aatn
-- Granularidad: una fila por empresa
-- PK: tenant_id + company_id
{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'company_id'],
        incremental_strategy='merge',
        cluster_by=['tenant_id']
    )
}}
WITH
-- -----------------------------------------------------------------------
-- 1. Companies PF
-- -----------------------------------------------------------------------
pf_companies AS (
    SELECT * FROM {{ ref('stg_pf__companies') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),
pf_deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, source_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM pf_companies
    )
    WHERE rn = 1
),
-- -----------------------------------------------------------------------
-- 2. Company Access PF
-- -----------------------------------------------------------------------
company_access AS (
    SELECT * FROM {{ ref('stg_pf__company_access') }}
),
ranked_by_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY company_id
            ORDER BY total_sucursales_del_acceso ASC, ingested_at DESC
        ) AS rn
    FROM company_access
),
resolved_by_id AS (
    SELECT
        company_id      AS resolved_company_id,
        rut_company     AS resolved_rut,
        access_email    AS contact_email,
        email_status    AS contact_email_status
    FROM ranked_by_id
    WHERE rn = 1
),
ranked_by_rut AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY rut_company
            ORDER BY total_sucursales_del_acceso ASC, ingested_at DESC
        ) AS rn_rut
    FROM company_access
),
resolved_by_rut AS (
    SELECT
        rut_company     AS resolved_rut,
        access_email    AS contact_email,
        email_status    AS contact_email_status
    FROM ranked_by_rut
    WHERE rn_rut = 1
),
-- -----------------------------------------------------------------------
-- 3. PF Output
-- -----------------------------------------------------------------------
pf_final AS (
    SELECT
        pf_deduped.tenant_id,
        pf_deduped.source_id                                        AS company_id,
        pf_deduped.company_name,
        pf_deduped.company_email,
        pf_deduped.status,
        pf_deduped.city,
        pf_deduped.region,
        pf_deduped.rut_company,
        pf_deduped.company_code,
        pf_deduped.oficina_venta,
        CASE
            WHEN by_id.contact_email IS NOT NULL
            AND NOT REGEXP_CONTAINS(LOWER(by_id.contact_email), r'pfalimentos\.|pfaimentos\.|@pf\.cl$')
            THEN by_id.contact_email
            WHEN by_rut.contact_email IS NOT NULL
            AND NOT REGEXP_CONTAINS(LOWER(by_rut.contact_email), r'pfalimentos\.|pfaimentos\.|@pf\.cl$')
            THEN by_rut.contact_email
            ELSE pf_deduped.company_email
        END                                                         AS contact_email,
        CASE
            WHEN (
                (by_id.contact_email IS NOT NULL AND NOT REGEXP_CONTAINS(LOWER(by_id.contact_email), r'pfalimentos\.|pfaimentos\.|@pf\.cl$'))
                OR
                (by_rut.contact_email IS NOT NULL AND NOT REGEXP_CONTAINS(LOWER(by_rut.contact_email), r'pfalimentos\.|pfaimentos\.|@pf\.cl$'))
            )
            THEN 'company_access'
            ELSE 'magento_registration_fallback'
        END                                                         AS contact_email_source,
        COALESCE(by_id.contact_email_status, by_rut.contact_email_status) AS contact_email_status,
        pf_deduped.bronze_id,
        pf_deduped.ingested_at
    FROM pf_deduped
    LEFT JOIN resolved_by_id AS by_id
        ON SAFE_CAST(pf_deduped.source_id AS INT64) = by_id.resolved_company_id
    LEFT JOIN resolved_by_rut AS by_rut
        ON pf_deduped.rut_company = by_rut.resolved_rut
),
-- -----------------------------------------------------------------------
-- 4. Companies AATN
-- -----------------------------------------------------------------------
aatn_raw AS (
    SELECT * FROM {{ ref('stg_aatn__companies') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),
aatn_deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, source_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM aatn_raw
    )
    WHERE rn = 1
),
aatn_final AS (
    SELECT
        tenant_id,
        source_id                                                   AS company_id,
        company_name,
        company_email,
        status,
        city,
        region,
        rut_company,
        company_code,
        oficina_venta,
        company_email                                               AS contact_email,
        'magento_registration_fallback'                             AS contact_email_source,
        CAST(NULL AS STRING)                                        AS contact_email_status,
        bronze_id,
        ingested_at
    FROM aatn_deduped
),
-- -----------------------------------------------------------------------
-- 5. Union final
-- -----------------------------------------------------------------------
unioned AS (
    SELECT * FROM pf_final
    UNION ALL
    SELECT * FROM aatn_final
),
deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, company_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM unioned
    )
    WHERE rn = 1
)
SELECT * FROM deduped
