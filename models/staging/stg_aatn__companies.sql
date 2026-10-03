-- models/staging/aatn/stg_aatn__companies.sql
-- Normaliza empresas Ariztía desde bronze.ecommerce (entity='companies')
-- Fuente: Endpoint custom de Ariztía — extrae atributos de empresa B2B
-- Materialización: view — solo parseo, sin dedupe
{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id          AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'companies'
      AND tenant_id = 'aatn'
)
SELECT
    bronze_id,
    tenant_id,
    ingested_at,
    JSON_VALUE(raw_json, '$.extension_attributes.sap_id')                       AS source_id,
    JSON_VALUE(raw_json, '$.company_name')                                      AS company_name,
    LOWER(JSON_VALUE(raw_json, '$.company_email'))                              AS company_email,
    CASE JSON_VALUE(raw_json, '$.status')
        WHEN '1' THEN 'ACTIVE'
        WHEN '3' THEN 'BLOCKED'
        ELSE 'INACTIVE'
    END                                                                         AS status,
    INITCAP(JSON_VALUE(raw_json, '$.city'))                                     AS city,
    cr.region                                                                   AS region,
    JSON_VALUE(raw_json, '$.extension_attributes.rut_company')                  AS rut_company,
    JSON_VALUE(raw_json, '$.extension_attributes.parent_id')                    AS company_code,
    JSON_VALUE(raw_json, '$.extension_attributes.centro_de_distribucion')       AS oficina_venta,
    JSON_VALUE(raw_json, '$.extension_attributes.codigo_bloqueo')               AS codigo_bloqueo,
    JSON_VALUE(raw_json, '$.pipeline_ingested_at')                              AS pipeline_ingested_at,
    -- Sin equivalente a company_access para Ariztía todavía
    CAST(NULL AS STRING)                                                        AS contact_email,
    CAST(NULL AS STRING)                                                        AS contact_email_source,
    CAST(NULL AS STRING)                                                        AS contact_email_status
FROM bronze
LEFT JOIN {{ ref('comunas_regiones') }} cr
    ON {{ normalize_string("SPLIT(JSON_VALUE(raw_json, '$.city'), ' - ')[OFFSET(0)]") }}
     = {{ normalize_string('cr.comunas') }}
WHERE JSON_VALUE(raw_json, '$.id') IS NOT NULL
