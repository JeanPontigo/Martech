-- models/staging/aatn/stg_aatn__clients.sql
-- Normaliza catálogo de clientes Ariztía desde bronze.ecommerce (entity='clients')
-- Fuente: Endpoint custom de Ariztía — extrae atributos de perfil, empresa y contacto.
-- Materialización: view — parseo y extracción de JSON sin deduplicación.

{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'clients'
      AND tenant_id = 'aatn'
)

SELECT
    bronze_id,
    tenant_id,
    ingested_at,

    -- Identificadores
    JSON_VALUE(raw_json, '$.entity_id')                                 AS client_id,
    JSON_VALUE(raw_json, '$.adobe_id')                                  AS adobe_id,
    JSON_VALUE(raw_json, '$.sap_id')                                    AS erp_id,
    JSON_VALUE(raw_json, '$.rut_company')                               AS tax_id,

    -- Perfil y Datos de Contacto
    JSON_VALUE(raw_json, '$.tipo_cliente')                              AS customer_type,
    JSON_VALUE(raw_json, '$.razon_social')                              AS company_name,
    JSON_VALUE(raw_json, '$.contacto')                                  AS contact_name,
    JSON_VALUE(raw_json, '$.email')                                     AS email,
    JSON_VALUE(raw_json, '$.celular')                                   AS phone,
    JSON_VALUE(raw_json, '$.centro')                                    AS distribution_center,
    JSON_VALUE(raw_json, '$.region')                                    AS region,
    CAST(JSON_VALUE(raw_json, '$.status') AS INT64)                     AS status,

    -- NOTA: Se excluye el campo $.password por buenas prácticas de seguridad.

    -- Fechas y Auditoría
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_VALUE(raw_json, '$.created_at'))  AS created_at,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_VALUE(raw_json, '$.last_login'))  AS last_login_at

FROM bronze
WHERE JSON_VALUE(raw_json, '$.entity_id') IS NOT NULL
