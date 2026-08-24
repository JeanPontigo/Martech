-- models/silver/fact_order_items.sql
-- Fuente: staging normalizado por tenant (ver models/staging/)
-- Tenant: pf, aatn
-- Granularidad: una fila por item por orden
-- PK: (tenant_id, order_id, sku) — llave compuesta natural, sin hash
-- FK: (tenant_id, order_id) -> fact_orders (client_id y status viven ahí,
--     no se duplican en esta tabla — hacer JOIN cuando se necesiten)
--
-- Fuentes normalizadas:
--   pf   → {{ ref('stg_pf__order_items') }}    (nativo, insert + update)
--   aatn → {{ ref('stg_aatn__order_items') }}  (custom, fuente única —
--          las órdenes de Ariztía no se modifican después de creadas)

{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'order_id', 'sku'],
        incremental_strategy='merge',
        partition_by={
            'field': 'ingested_at',
            'data_type': 'datetime',
            'granularity': 'month'
        },
        cluster_by=['tenant_id']
    )
}}

WITH

pf_raw AS (
    SELECT * FROM {{ ref('stg_pf__order_items') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

aatn_raw AS (
    SELECT * FROM {{ ref('stg_aatn__order_items') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

mc_raw AS (
    SELECT * FROM {{ ref('stg_mc__order_items') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

unioned AS (
    SELECT
        tenant_id, order_id, sku, product_type,
        qty_ordered, qty_canceled, qty_invoiced, qty_shipped,
        original_price, unit_price, line_total_net,
        discount_amount, discount_pct,
        list_price_discounted, discount_applied,
        created_at, updated_at, bronze_id, ingested_at
    FROM pf_raw

    UNION ALL

    SELECT
        tenant_id, order_id, sku, product_type,
        qty_ordered, qty_canceled, qty_invoiced, qty_shipped,
        original_price, unit_price, line_total_net,
        discount_amount, discount_pct,
        list_price_discounted, discount_applied,
        created_at, updated_at, bronze_id, ingested_at
    FROM aatn_raw

    UNION ALL

    SELECT
        tenant_id, order_id, sku, product_type,
        qty_ordered, qty_canceled, qty_invoiced, qty_shipped,
        original_price, unit_price, line_total_net,
        discount_amount, discount_pct,
        list_price_discounted, discount_applied,
        created_at, updated_at, bronze_id, ingested_at
    FROM mc_raw
    
),

deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, order_id, sku
                ORDER BY ingested_at DESC
            ) AS rn
        FROM unioned
    )
    WHERE rn = 1
)

SELECT
    tenant_id,
    order_id,
    sku,
    product_type,
    qty_ordered,
    qty_canceled,
    qty_invoiced,
    qty_shipped,
    original_price,
    unit_price,
    list_price_discounted,
    discount_amount,
    discount_pct,
    discount_applied,
    line_total_net,
    bronze_id,
    ingested_at
FROM deduped
