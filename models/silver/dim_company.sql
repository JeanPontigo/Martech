-- models/silver/dim_company.sql
-- Fuente: staging normalizado por tenant (ver models/staging/)
-- Tenant: pf
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
deduped AS (
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
-- -----------------------------------------------------------------------
-- 3. Elegir contacto más específico por company_id
-- -----------------------------------------------------------------------
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
-- -----------------------------------------------------------------------
-- 4. Fallback por rut_company
-- -----------------------------------------------------------------------
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
)
-- -----------------------------------------------------------------------
-- 5. Output final
-- -----------------------------------------------------------------------
SELECT
    deduped.tenant_id,
    deduped.source_id                                           AS company_id,
    deduped.company_name,
    deduped.company_email,
    deduped.status,
    deduped.city,
    deduped.region,
    deduped.rut_company,
    deduped.company_code,
    deduped.oficina_venta,
    CASE
        WHEN by_id.contact_email IS NOT NULL
        AND NOT REGEXP_CONTAINS(LOWER(by_id.contact_email), r'pfalimentos\.|pfaimentos\.|@pf\.cl$')
        THEN by_id.contact_email
        WHEN by_rut.contact_email IS NOT NULL
        AND NOT REGEXP_CONTAINS(LOWER(by_rut.contact_email), r'pfalimentos\.|pfaimentos\.|@pf\.cl$')
        THEN by_rut.contact_email
        ELSE deduped.company_email
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
    deduped.bronze_id,
    deduped.ingested_at
FROM deduped
LEFT JOIN resolved_by_id AS by_id
    ON SAFE_CAST(deduped.source_id AS INT64) = by_id.resolved_company_id
LEFT JOIN resolved_by_rut AS by_rut
    ON deduped.rut_company = by_rut.resolved_rut
