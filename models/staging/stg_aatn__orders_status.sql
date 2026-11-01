-- models/staging/stg_aatn__orders_status.sql
-- Normaliza cambios de status de órdenes Ariztía desde bronze.ecommerce
-- (entity='orders_updated'). Fuente: 100% endpoint nativo Magento.
-- Overlay parcial: aporta status/updated_at. El nativo no trae company_id
-- como campo propio, pero SÍ lleva el id de empresa de Magento incrustado
-- en el correo del cliente (formato usuario+<company_code>@dominio) —
-- se expone aquí para que fact_orders pueda recuperar la empresa de las
-- órdenes históricas donde el custom nunca entregó company_id.
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

    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.created_at')))                        AS created_at_utc,

    DATETIME(TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at')))                        AS updated_at,
    
    JSON_VALUE(raw_json, '$.billing_address.region')                AS region_nativa,

    JSON_VALUE(raw_json, '$.customer_email')                                         AS customer_email_nativo,

    JSON_VALUE(raw_json, '$.extension_attributes.company_order_attributes.company_id') AS company_id_nativo

FROM bronze
