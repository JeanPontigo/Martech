-- models/staging/mc/stg_mc__clients.sql
-- Normaliza clientes Carozzi desde bronze.ecommerce (entity IN 'clients','clients_updated')
-- Fuente: 100% nativo Magento
-- Diferencias vs PF: rut viene en 'rut_register' no 'rut', sin amcompany_attributes
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
      AND tenant_id = 'mc'
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
        WHEN '0' THEN 'NOT LOGGED IN'
        WHEN '1' THEN 'General'
        WHEN '4' THEN 'Primera compra'
        WHEN '5' THEN 'Beneficios'
        WHEN '6' THEN 'Carozzino'
        ELSE 'Sin Grupo'
    END                                                         AS group_name,
    -- rut viene en rut_register (diferente a PF que usa rut)
    (
        SELECT JSON_VALUE(attr, '$.value')
        FROM UNNEST(JSON_QUERY_ARRAY(raw_json, '$.custom_attributes')) AS attr
        WHERE JSON_VALUE(attr, '$.attribute_code') = 'rut_register'
        LIMIT 1
    )                                                           AS rut,
    -- Carozzi no tiene B2B companies — campos NULL
    CAST(NULL AS STRING)                                        AS company_id,
    CAST(NULL AS STRING)                                        AS company_role_id,
    CAST(NULL AS STRING)                                        AS company_job_title,
    -- Sin cuentas técnicas identificadas en Carozzi
    FALSE                                                       AS is_technical_account,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))  AS created_at,
    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))  AS updated_at
FROM bronze
