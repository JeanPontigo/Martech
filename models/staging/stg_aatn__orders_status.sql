-- models/staging/aatn/stg_aatn__orders_status.sql
-- Normaliza cambios de status de órdenes Ariztía desde bronze.ecommerce
-- (entity='orders_updated'). Fuente: 100% endpoint nativo Magento.
-- Overlay parcial: SOLO aporta status/updated_at. El nativo no tiene
-- company_id/client_id/centro/comuna/sap_id — nunca reemplaza la fila
-- completa de stg_aatn__orders_custom.
-- Materialización: view.

{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'orders_updated'
      AND tenant_id = 'aatn'
)

SELECT
    bronze_id,
    tenant_id,
    ingested_at,

    JSON_VALUE(raw_json, '$.increment_id')                                          AS order_id,

    -- PENDIENTE DE VALIDACIÓN: vocabulario de status del nativo para aatn
    -- sin confirmar contra JSON real — mismo mapeo que pf como supuesto.
    JSON_VALUE(raw_json, '$.status')                                                 AS status,

    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))                        AS updated_at

FROM bronze