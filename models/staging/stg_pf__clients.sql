-- models/staging/pf/stg_pf__clients.sql
-- Normaliza clientes PF desde bronze.ecommerce (entity IN 'clients','clients_updated')
-- Fuente: 100% nativo Magento
-- Materialización: view — solo parseo, sin dedupe
{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity IN ('clients', 'clients_updated')
      AND tenant_id = 'pf'
)
SELECT
    bronze_id,
    tenant_id,
    ingested_at,
    JSON_VALUE(raw_json, '$.id')                                AS source_id,
    JSON_VALUE(raw_json, '$.email')                             AS email_raw,
    LOWER(JSON_VALUE(raw_json, '$.email'))                      AS email,
    LOWER(
        CASE
            WHEN JSON_VALUE(raw_json, '$.email') LIKE '%+%'
            THEN CONCAT(
                SPLIT(JSON_VALUE(raw_json, '$.email'), '+')[OFFSET(0)],
                '@',
                SPLIT(JSON_VALUE(raw_json, '$.email'), '@')[OFFSET(1)]
            )
            ELSE JSON_VALUE(raw_json, '$.email')
        END
    )                                                           AS email_clean,
    INITCAP(JSON_VALUE(raw_json, '$.firstname'))                AS first_name,
    INITCAP(JSON_VALUE(raw_json, '$.lastname'))                 AS last_name,
    JSON_VALUE(raw_json, '$.group_id')                          AS group_id,
    CASE JSON_VALUE(raw_json, '$.group_id')
        WHEN '1'  THEN 'GENERAL'
        WHEN '14' THEN 'SUPERMERCADO'
        WHEN '15' THEN 'FOODSERVICE'
        WHEN '16' THEN 'TRADICIONAL'
        ELSE 'Sin Grupo'
    END                                                         AS group_name,
    (
        SELECT JSON_VALUE(attr, '$.value')
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.custom_attributes')) AS attr
        WHERE JSON_VALUE(attr, '$.attribute_code') = 'rut'
        LIMIT 1
    )                                                           AS rut,
    JSON_VALUE(raw_json, '$.extension_attributes.amcompany_attributes.company_id')
                                                                AS company_id,
    JSON_VALUE(raw_json, '$.extension_attributes.amcompany_attributes.role_id')
                                                                AS company_role_id,
    JSON_VALUE(raw_json, '$.extension_attributes.amcompany_attributes.job_title')
                                                                AS company_job_title,
    (
        REGEXP_CONTAINS(LOWER(JSON_VALUE(raw_json, '$.email')), r'\+\d+@')
        OR REGEXP_CONTAINS(LOWER(JSON_VALUE(raw_json, '$.email')), r'^admin[a-z0-9]+@pfalimentos\.cl$')
    )                                                           AS is_technical_account,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))  AS created_at,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))  AS updated_at
FROM bronze
