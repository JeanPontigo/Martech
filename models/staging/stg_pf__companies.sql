-- models/staging/pf/stg_pf__companies.sql
-- Normaliza empresas PF desde bronze.ecommerce (entity='companies')
-- Fuente: endpoint Amasty /rest/V1/amcompany/company/search
-- Materialización: view — solo parseo, sin dedupe
{{ config(materialized='view') }}

SELECT
    id          AS bronze_id,
    tenant_id,
    ingested_at,
    JSON_VALUE(raw_json, '$.company_id')                        AS source_id,
    INITCAP(JSON_VALUE(raw_json, '$.company_name'))             AS company_name,
    LOWER(JSON_VALUE(raw_json, '$.company_email'))              AS company_email,
    CASE JSON_VALUE(raw_json, '$.status')
        WHEN '1' THEN 'ACTIVE'
        ELSE 'INACTIVE'
    END                                                         AS status,
    INITCAP(JSON_VALUE(raw_json, '$.city'))                     AS city,
    INITCAP(JSON_VALUE(raw_json, '$.region'))                   AS region,
    JSON_VALUE(raw_json, '$.rut_company')                       AS rut_company,
    JSON_VALUE(raw_json, '$.codigo_cliente')                    AS company_code,
    JSON_VALUE(raw_json, '$.codigo_stock')                      AS oficina_venta
FROM {{ source('bronze', 'ecommerce') }}
WHERE entity = 'companies'
  AND tenant_id = 'pf'
