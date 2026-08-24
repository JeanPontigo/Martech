-- models/staging/pf/stg_pf__company_access.sql
-- Normaliza accesos de contacto PF desde bronze.ecommerce (entity='company_access')
-- Fuente: endpoint custom /rest/V1/martech-extcompanyusers/access/search
-- Incluye expansión de sucursales y ranking por especificidad
-- Materialización: view — parseo + UNNEST de sucursales, sin dedupe final
{{ config(materialized='view') }}

WITH raw AS (
    SELECT
        JSON_VALUE(raw_json, '$.entity_id')         AS access_id,
        LOWER(JSON_VALUE(raw_json, '$.email'))      AS access_email,
        JSON_VALUE(raw_json, '$.email_status')      AS email_status,
        JSON_VALUE(raw_json, '$.rut_company')       AS rut_company,
        ingested_at,
        JSON_VALUE(raw_json, '$.sucursales')        AS sucursales_str
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'company_access'
      AND tenant_id = 'pf'
),
exploded AS (
    SELECT
        access_id,
        access_email,
        email_status,
        rut_company,
        ingested_at,
        SAFE_CAST(JSON_VALUE(sucursal) AS INT64) AS company_id
    FROM raw,
    UNNEST(JSON_VALUE_ARRAY(sucursales_str)) AS sucursal
)
SELECT
    access_id,
    access_email,
    email_status,
    rut_company,
    ingested_at,
    company_id,
    COUNT(*) OVER (PARTITION BY access_id) AS total_sucursales_del_acceso
FROM exploded
