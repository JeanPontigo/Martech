-- models/staging/aatn/stg_aatn__orders_custom.sql
-- Normaliza órdenes Ariztía desde bronze.ecommerce (entity='orders')
-- Fuente: 100% endpoint custom — fuente de verdad para identidad y detalle
-- (company_id, client_id, centro, comuna, sap_id). NO incluye status
-- actualizado — eso vive en stg_aatn__orders_status.sql (overlay).
-- Materialización: view — solo parseo de JSON, sin dedupe.
--
-- NOTA: el custom deja de entregar customer_sap_id, company_id y comuna
-- cuando la orden pasa a 'facturado'. Como el dedupe de fact_orders se queda
-- con la carga más reciente, esos campos se perdían. Se rellenan con el
-- primer valor no nulo de la misma orden.
--
-- NOTA 2 (2026-10-21): el vaciado al facturar tambien afecta el monto de las
-- lineas, y subtotal_net se calculaba sumando los items del raw_json de la
-- carga actual — en la carga facturada, cero. Ahora se toma desde
-- stg_aatn__order_items, que rellena cada linea con su primer monto distinto
-- de cero. Efecto secundario deseado: el subtotal de la orden y la suma de
-- sus items quedan iguales por construccion, no por coincidencia.

{{ config(materialized='view') }}

WITH bronze AS (
    SELECT
        id              AS bronze_id,
        tenant_id,
        raw_json,
        ingested_at
    FROM {{ source('bronze', 'ecommerce') }}
    WHERE entity = 'orders'
      AND tenant_id = 'aatn'
),

relleno AS (
    SELECT
        bronze_id,
        tenant_id,
        raw_json,
        ingested_at,
        JSON_VALUE(raw_json, '$.order_id')                                      AS order_id,
        FIRST_VALUE(JSON_VALUE(raw_json, '$.customer_sap_id') IGNORE NULLS) OVER w AS client_id,
        FIRST_VALUE(JSON_VALUE(raw_json, '$.company_id')      IGNORE NULLS) OVER w AS company_id,
        FIRST_VALUE(JSON_VALUE(raw_json, '$.comuna')          IGNORE NULLS) OVER w AS comuna
    FROM bronze
    WINDOW w AS (
        PARTITION BY JSON_VALUE(raw_json, '$.order_id')
        ORDER BY ingested_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

-- -----------------------------------------------------------------------
-- Una fila por linea, quedandose con la carga mas reciente. El monto ya
-- viene rellenado desde stg_aatn__order_items.
-- -----------------------------------------------------------------------
items_dedup AS (
    SELECT
        order_id,
        item_id,
        line_total_net
    FROM {{ ref('stg_aatn__order_items') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id, item_id
        ORDER BY ingested_at DESC
    ) = 1
),

montos_orden AS (
    SELECT
        order_id,
        SUM(line_total_net)     AS subtotal_net
    FROM items_dedup
    GROUP BY order_id
),
-- -----------------------------------------------------------------------
-- Comunas oficiales mas la tabla de equivalencias (provincias, localidades,
-- nombres truncados por el origen y errores de tipeo). El campo de comuna
-- del sitio de Ariztia es texto libre, por eso la cola larga.
-- Ante un nombre presente en ambas tablas gana comunas_regiones.
-- -----------------------------------------------------------------------
geo AS (
    SELECT region, nombre
    FROM (
        SELECT region, comunas AS nombre, 1 AS prioridad
        FROM {{ ref('comunas_regiones') }}
        UNION ALL
        SELECT region, nombre,            2 AS prioridad
        FROM {{ ref('comunas_alias') }}
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ normalize_string('nombre') }}
        ORDER BY prioridad
    ) = 1
)
    
SELECT
    r.bronze_id,
    r.tenant_id,
    r.ingested_at,
    r.order_id,
    JSON_VALUE(r.raw_json, '$.sap_id')                                              AS incremental_id,
    r.client_id,
    JSON_VALUE(r.raw_json, '$.email')                                               AS customer_email,
    r.company_id,
    DATETIME(
        CONCAT(JSON_VALUE(r.raw_json, '$.fecha_compra'), ' ', JSON_VALUE(r.raw_json, '$.hora_compra'))
    )                                                                                AS created_at_local,
    JSON_VALUE(r.raw_json, '$.status')                                               AS status,
    INITCAP(r.comuna)                                                                AS ciudad_compra,
    mo.subtotal_net                                                                  AS subtotal_net,
    CAST(NULL AS FLOAT64)                                                            AS discount_amount,
    CAST(NULL AS STRING)                                                             AS discount_pct,
    CAST(NULL AS FLOAT64)                                                            AS shipping_amount,
    CAST(NULL AS STRING)                                                             AS courier,
    (
        SELECT JSON_VALUE(item, '$.nombre_cupon')
        FROM UNNEST(JSON_QUERY_ARRAY(r.raw_json, '$.items')) AS item
        WHERE JSON_VALUE(item, '$.nombre_cupon') IS NOT NULL
        LIMIT 1
    )                                                                                AS coupon_code,
    CAST(NULL AS STRING)                                                             AS medio_pago,
    geo.region                                                                       AS region_compra,
    JSON_VALUE(r.raw_json, '$.sap_id')                                               AS erp_id,
    CAST(NULL AS STRING)                                                             AS gifcard_code,
    'ecommerce'                                                                      AS canal_compra
FROM relleno r
LEFT JOIN montos_orden mo
    ON r.order_id = mo.order_id
LEFT JOIN geo
    ON {{ normalize_string("SPLIT(r.comuna, ' - ')[OFFSET(0)]") }}
     = {{ normalize_string('geo.nombre') }}
