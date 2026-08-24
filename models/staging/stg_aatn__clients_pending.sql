-- models/staging/aatn/stg_aatn__clients_pending.sql
-- Normaliza prospectos o solicitudes de clientes pendientes Ariztía desde bronze.ecommerce (entity='clients_pending')
-- Fuente: Endpoint custom de Ariztía — extrae solicitudes en proceso de alta.
-- Materialización: view — parseo y extracción de JSON sin deduplicación.

{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'clients_pending'
      AND tenant_id = 'aatn'
)

SELECT
    bronze_id,
    tenant_id,
    ingested_at,

    -- Identificadores
    JSON_VALUE(raw_json, '$.id_adobe')                                  AS adobe_id,
    JSON_VALUE(raw_json, '$.id_sap')                                    AS erp_id,
    JSON_VALUE(raw_json, '$.usu_rut')                                   AS tax_id,

    -- Datos de la Solicitud
    JSON_VALUE(raw_json, '$.pros_cli_razon_social')                     AS company_name,
    JSON_VALUE(raw_json, '$.pros_cli_contacto')                         AS contact_name,
    JSON_VALUE(raw_json, '$.pros_cli_mail')                             AS email,
    JSON_VALUE(raw_json, '$.pros_cli_celular')                          AS phone,

    -- Fechas
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', JSON_VALUE(raw_json, '$.pros_cli_fecha_solicitud')) AS requested_at

FROM bronze
WHERE JSON_VALUE(raw_json, '$.id_adobe') IS NOT NULL 
   OR JSON_VALUE(raw_json, '$.usu_rut') IS NOT NULL
