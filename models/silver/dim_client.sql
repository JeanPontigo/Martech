-- models/silver/dim_client.sql
-- Fuente: staging normalizado por tenant (ver models/staging/)
-- Tenant: pf, mc, aatn
-- Granularidad: una fila por cliente
-- PK: tenant_id + client_id
{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'client_id'],
        incremental_strategy='merge',
        cluster_by=['tenant_id']
    )
}}
WITH
pf_raw AS (
    SELECT * FROM {{ ref('stg_pf__clients') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),
mc_raw AS (
    SELECT * FROM {{ ref('stg_mc__clients') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),
aatn_clients AS (
    SELECT
        c.bronze_id, c.tenant_id, c.ingested_at,
        client_id                                    AS source_id,
        c.email                                      AS email_raw,
        LOWER(c.email)                               AS email,
        LOWER(c.email)                               AS email_clean,
        c.contact_name                               AS first_name,
        CAST(NULL AS STRING)                         AS last_name,
        CAST(NULL AS STRING)                         AS group_id,
        c.customer_type                              AS group_name,
        c.tax_id                                     AS rut,
        co.source_id                                 AS company_id,
        CAST(NULL AS STRING)                         AS company_role_id,
        CAST(NULL AS STRING)                         AS company_job_title,
        FALSE                                        AS is_technical_account,
        CAST(c.created_at AS DATETIME)               AS created_at,
        CAST(NULL AS DATETIME)                       AS updated_at
    FROM {{ ref('stg_aatn__clients') }} c
    LEFT JOIN {{ ref('stg_aatn__companies') }} co
        ON c.tax_id = co.rut_company
        AND c.tenant_id = co.tenant_id
),
aatn_pending AS (
    SELECT
        bronze_id, tenant_id, ingested_at,
        adobe_id                                    AS source_id,
        email                                       AS email_raw,
        LOWER(email)                                AS email,
        LOWER(email)                                AS email_clean,
        contact_name                                AS first_name,
        CAST(NULL AS STRING)                        AS last_name,
        CAST(NULL AS STRING)                        AS group_id,
        'PROSPECTO'                                 AS group_name,
        tax_id                                      AS rut,
        CAST(NULL AS STRING)                        AS company_id,
        CAST(NULL AS STRING)                        AS company_role_id,
        CAST(NULL AS STRING)                        AS company_job_title,
        FALSE                                       AS is_technical_account,
        CAST(requested_at AS DATETIME)              AS created_at,
        CAST(NULL AS DATETIME)                      AS updated_at
    FROM {{ ref('stg_aatn__clients_pending') }}
),
aatn_raw AS (
    SELECT * FROM aatn_clients
    UNION ALL
    SELECT * FROM aatn_pending
),
unioned AS (
    SELECT
        bronze_id, tenant_id, ingested_at,
        source_id, email_raw, email, email_clean,
        first_name, last_name, group_id, group_name,
        rut, company_id, company_role_id, company_job_title,
        is_technical_account, created_at, updated_at
    FROM pf_raw
    UNION ALL
    SELECT
        bronze_id, tenant_id, ingested_at,
        source_id, email_raw, email, email_clean,
        first_name, last_name, group_id, group_name,
        rut, company_id, company_role_id, company_job_title,
        is_technical_account, created_at, updated_at
    FROM mc_raw
    UNION ALL
    SELECT
        bronze_id, tenant_id, ingested_at,
        source_id, email_raw, email, email_clean,
        first_name, last_name, group_id, group_name,
        rut, company_id, company_role_id, company_job_title,
        is_technical_account, created_at, updated_at
    FROM aatn_raw
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
        FROM unioned
    )
    WHERE rn = 1
)
SELECT
    tenant_id,
    source_id                                                   AS client_id,
    email,
    email_clean,
    first_name,
    last_name,
    group_id,
    group_name,
    rut,
    company_id,
    company_role_id,
    company_job_title,
    is_technical_account,
    created_at,
    updated_at,
    DATETIME(TIMESTAMP(created_at), 'America/Santiago')         AS created_at_chile,
    bronze_id,
    ingested_at
FROM deduped
