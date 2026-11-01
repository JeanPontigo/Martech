-- models/staging/aatn/stg_aatn__order_items.sql
-- Normaliza líneas de orden Ariztía desde bronze.ecommerce (entity='orders')
-- Fuente: 100% endpoint custom — items[] anidados dentro de cada orden.
--
-- ATENCION: el payload SI cambia entre cargas. Al pasar a 'facturado' el
-- endpoint vacia campos que entregaba antes. Confirmado 2026-10-21: las
-- lineas con monto en cero aparecen solo en los estados finales 'facturado'
-- (3,4% de sus lineas) y 'closed' (3,3%), y en 0% de 'ingresado', 'canceled'
-- y 'en_ruta'. Un producto gratis seria gratis desde el ingreso, por lo que
-- el cero es perdida del dato y no precio real. Mismo patron que
-- customer_sap_id, company_id y comuna en stg_aatn__orders_custom.
--
-- Por eso el CTE 'relleno': toma el primer valor distinto de cero de cada
-- linea entre todas sus cargas. El cero se trata como ausente via NULLIF,
-- porque el endpoint entrega 0 y no NULL.
--
-- Alcance de la recuperacion (medido 2026-10-21): 1.184 lineas recuperadas,
-- 226,7 millones de pesos. Quedan 15.817 que solo se vieron ya facturadas y
-- cuyo precio no esta en bronze — se estiman en 774 millones, 1,9% de la
-- venta del tenant, usando el precio promedio del mismo SKU. Hacia adelante
-- el job diario captura 'ingresado' antes del vaciado.
--
-- DEPENDENCIA: stg_aatn__orders_custom calcula su subtotal_net sumando
-- line_total_net de esta vista, en lugar de leer el raw_json de la carga.
-- Por eso el subtotal de la orden y la suma de sus items son iguales por
-- construccion. Cambiar el relleno de aqui mueve el ingreso de Ariztia.
--
-- Materialización: view — solo parseo/UNNEST, sin dedupe. El dedupe por
-- (tenant_id, order_id, item_id) lo hace fact_order_items.

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

unnested AS (
    SELECT
        bronze_id,
        tenant_id,
        ingested_at,
        raw_json,
        item
    FROM bronze,
    UNNEST(JSON_QUERY_ARRAY(raw_json, '$.items')) AS item
),

-- -----------------------------------------------------------------------
-- Rellenar los montos vaciados al facturar, tomando el primer valor con
-- precio entre todas las cargas de la misma linea.
-- -----------------------------------------------------------------------
relleno AS (
    SELECT
        bronze_id,
        tenant_id,
        ingested_at,
        raw_json,
        item,
        JSON_VALUE(raw_json, '$.order_id')                                  AS order_id,
        JSON_VALUE(item, '$.item_id')                                       AS item_id,
        FIRST_VALUE(
            NULLIF(SAFE_CAST(JSON_VALUE(item, '$.price') AS FLOAT64), 0)
            IGNORE NULLS
        ) OVER w                                                            AS unit_price_relleno,
        FIRST_VALUE(
            NULLIF(SAFE_CAST(JSON_VALUE(item, '$.venta_neta') AS FLOAT64), 0)
            IGNORE NULLS
        ) OVER w                                                            AS line_total_net_relleno,
        FIRST_VALUE(
            NULLIF(SAFE_CAST(JSON_VALUE(item, '$.monto_descuento') AS FLOAT64), 0)
            IGNORE NULLS
        ) OVER w                                                            AS descuento_relleno
    FROM unnested
    WINDOW w AS (
        PARTITION BY
            JSON_VALUE(raw_json, '$.order_id'),
            JSON_VALUE(item, '$.item_id')
        ORDER BY ingested_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)

SELECT
    bronze_id,
    tenant_id,
    ingested_at,

    order_id,
    item_id,
    JSON_VALUE(item, '$.sku')                                                       AS sku,

    -- PENDIENTE: el custom de Ariztía no entrega product_type ni
    -- parent_item_id (confirmado 2026-10-21 contra el JSON crudo).
    -- Solicitado a Ariztía. Sin esto no se pueden identificar packs.
    CAST(NULL AS STRING)                                                            AS product_type,

    CAST(JSON_VALUE(item, '$.cantidad') AS FLOAT64)                                 AS qty_ordered,

    -- Sin dato en el custom — no asumir equivalencia con qty_ordered.
    CAST(NULL AS FLOAT64)                                                           AS qty_canceled,
    CAST(NULL AS FLOAT64)                                                           AS qty_invoiced,
    CAST(NULL AS FLOAT64)                                                           AS qty_shipped,

    COALESCE(unit_price_relleno, 0.0)                                               AS unit_price,

    -- original_price no existe en el custom de Ariztía (confirmado) —
    -- sin este campo no se puede derivar list_price_discounted igual que pf.
    CAST(NULL AS FLOAT64)                                                           AS original_price,

    COALESCE(line_total_net_relleno, 0.0)                                           AS line_total_net,

    -- monto_descuento del payload custom: el descuento declarado por linea.
    -- Es informativo y NO entra en ninguna formula de ingreso: en aatn
    -- venta_neta ya es el monto cobrado, asi que line_total_net es neto y
    -- restar esto otra vez contaria doble. Aparece en 308 lineas por 2
    -- millones en total, asi que NO refleja todo el descuento real:
    -- descuento_promo trae ademas un porcentaje (2 a 25) en el 16,7% de las
    -- lineas que no explica la diferencia entre venta_neta y price*cantidad.
    -- Semantica consultada a Ariztia (2026-10-22). Leer como piso, no total.
    COALESCE(descuento_relleno, 0.0)                                                AS discount_amount,
    -- porcentaje_descuento viene 0 y descuento_promo tiene semantica dudosa:
    -- no se deriva un porcentaje hasta que Ariztia confirme.
    CAST(NULL AS FLOAT64)                                                           AS discount_pct,

    COALESCE(unit_price_relleno, 0.0)                                               AS list_price_discounted,

    COALESCE(descuento_relleno, 0.0) > 0                                            AS discount_applied,

    -- fecha_compra + hora_compra ya vienen en hora Chile local — mismo
    -- criterio que stg_aatn__orders_custom.
    DATETIME(
        CONCAT(JSON_VALUE(raw_json, '$.fecha_compra'), ' ', JSON_VALUE(raw_json, '$.hora_compra'))
    )                                                                                AS created_at,
    DATETIME(
        CONCAT(JSON_VALUE(raw_json, '$.fecha_compra'), ' ', JSON_VALUE(raw_json, '$.hora_compra'))
    )                                                                                AS updated_at

FROM relleno
WHERE order_id IS NOT NULL
