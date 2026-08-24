-- models/silver/fact_orders.sql
-- Fuente: staging normalizado por tenant (ver models/staging/)
-- Tenant: pf, aatn
-- Granularidad: una fila por orden
-- PK: (tenant_id, order_id) — llave compuesta natural, sin hash
-- FK: (tenant_id, client_id) -> dim_client | (tenant_id, company_id) -> dim_company
--
-- Fuentes normalizadas:
--   pf   → {{ ref('stg_pf__orders') }}            (nativo, insert + update)
--   aatn → {{ ref('stg_aatn__orders_custom') }}    (custom, base de identidad)
--        + {{ ref('stg_aatn__orders_status') }}    (nativo, overlay de status)
--
-- IMPORTANTE — manejo del overlay de status (aatn):
-- Cuando llega un cambio de status para una orden YA MERGEADA en corridas
-- anteriores, su fila base (custom) puede quedar fuera de la ventana
-- incremental de esta corrida. Si el MERGE recibiera solo el status sin
-- el resto de la identidad (company_id, client_id, centro, comuna), el
-- MERGE reemplazaría la fila completa y esos campos quedarían NULL.
-- Por eso, para toda orden con status_update en esta corrida, se re-trae
-- su fila base COMPLETA (sin filtro incremental) desde el staging —
-- garantiza que el MERGE siempre reciba filas íntegras.

{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'order_id'],
        incremental_strategy='merge',
        partition_by={
            'field': 'created_at',
            'data_type': 'datetime',
            'granularity': 'month'
        },
        cluster_by=['tenant_id']
    )
}}

WITH

-- =========================================================================
-- PF — fuente única, nativo, ya trae insert + update en la misma forma
-- =========================================================================
pf_raw AS (
    SELECT * FROM {{ ref('stg_pf__orders') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

pf_final AS (
    SELECT
        tenant_id,
        order_id,
        client_id,
        customer_email,
        company_id,
        incremental_id,  
        CAST(erp_id AS STRING)                                                        AS erp_id,
        CAST(gifcard_code AS STRING)                                                  AS gifcard_code,
        created_at,
        CASE
            WHEN region_compra LIKE '%Magallanes%' THEN DATETIME(TIMESTAMP(created_at), 'America/Punta_Arenas')
            ELSE DATETIME(TIMESTAMP(created_at), 'America/Santiago')
        END                                                                          AS created_at_chile,
        status,
        updated_at,
        subtotal_net,
        discount_amount,
        discount_pct,
        shipping_amount,
        CAST(courier AS STRING)                                                      AS courier,
        coupon_code,
        medio_pago,
        canal_compra,
        region_compra,
        ciudad_compra,
        bronze_id,
        ingested_at
    FROM pf_raw
),
-- =========================================================================
-- MC (Carozzi) — fuente única, nativo, insert + update
-- =========================================================================
    
mc_raw AS (
    SELECT * FROM {{ ref('stg_mc__orders') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),
    
mc_final AS (
    SELECT
        tenant_id,
        order_id,
        client_id,
        customer_email,
        CAST(NULL AS STRING)                                                         AS company_id,
        incremental_id,
        CAST(erp_id AS STRING)                                                       AS erp_id,
        CAST(gifcard_code AS STRING)                                                 AS gifcard_code,
        created_at,
        DATETIME(TIMESTAMP(created_at), 'America/Santiago')                          AS created_at_chile,
        status,
        updated_at,
        subtotal_net,
        discount_amount,
        discount_pct,
        shipping_amount,
        CAST(courier AS STRING)                                                     AS courier,
        coupon_code,
        medio_pago,
        canal_compra,
        region_compra,
        ciudad_compra,
        bronze_id,
        ingested_at
    FROM mc_raw
),
    
-- =========================================================================
-- AATN — status updates de esta corrida (overlay)
-- =========================================================================
aatn_status_raw AS (
    SELECT * FROM {{ ref('stg_aatn__orders_status') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

aatn_status_latest AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM aatn_status_raw
    )
    WHERE rn = 1
),

-- Órdenes con cambio de status en esta corrida — necesitan su base COMPLETA
aatn_orders_con_status_update AS (
    SELECT DISTINCT order_id FROM aatn_status_latest
),

-- =========================================================================
-- AATN — base de identidad (custom)
-- Incluye: filas nuevas de esta corrida (por ingested_at) + filas viejas
-- cuya orden tuvo cambio de status ahora (re-scan completo del staging,
-- sin filtro incremental, para esas órdenes puntuales).
-- =========================================================================
aatn_base_nuevas AS (
    SELECT * FROM {{ ref('stg_aatn__orders_custom') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

aatn_base_por_status_update AS (
    SELECT b.*
    FROM {{ ref('stg_aatn__orders_custom') }} b
    INNER JOIN aatn_orders_con_status_update s
        ON b.order_id = s.order_id
    {% if is_incremental() %}
    WHERE b.order_id NOT IN (SELECT order_id FROM aatn_base_nuevas)
    {% endif %}
),

aatn_base_combinada AS (
    SELECT * FROM aatn_base_nuevas
    UNION ALL
    SELECT * FROM aatn_base_por_status_update
),

aatn_base_deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, order_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM aatn_base_combinada
    )
    WHERE rn = 1
),

-- =========================================================================
-- AATN — combinar base + overlay de status
-- =========================================================================
aatn_final AS (
    SELECT
        b.tenant_id,
        b.order_id,
        b.client_id,
        b.customer_email,
        b.company_id,
        b.incremental_id,
        CAST(b.erp_id AS STRING)                                                    AS erp_id,
        CAST(b.gifcard_code AS STRING)                                              AS gifcard_code,
        b.created_at_local                                                          AS created_at,
        b.created_at_local                                                          AS created_at_chile,
        COALESCE(s.status, b.status)                                                AS status,
        COALESCE(s.updated_at, b.created_at_local)                                  AS updated_at,
        b.subtotal_net,
        b.discount_amount,
        b.discount_pct,
        b.shipping_amount,
        CAST(b.courier AS STRING)                                                   AS courier,
        b.coupon_code,
        b.medio_pago,
        b.canal_compra,
        b.region_compra,
        b.ciudad_compra,
        b.bronze_id,
        GREATEST(b.ingested_at, COALESCE(s.ingested_at, b.ingested_at))             AS ingested_at
    FROM aatn_base_deduped b
    LEFT JOIN aatn_status_latest s
        ON b.order_id = s.order_id
),

-- =========================================================================
-- Unión de todas las fuentes normalizadas
-- =========================================================================
unioned AS (
    SELECT * FROM pf_final
    UNION ALL
    SELECT * FROM aatn_final
    UNION ALL
    SELECT * FROM mc_final
),

deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, order_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM unioned
    )
    WHERE rn = 1
),

-- -----------------------------------------------------------------------
-- Bridge a client_id por email — solo pf (client_id nativo puede venir
-- NULL en órdenes de invitado). aatn ya trae client_id resuelto
-- (customer_sap_id) desde el custom, no requiere fallback.
-- -----------------------------------------------------------------------
with_company AS (
    SELECT
        d.tenant_id,
        d.order_id,
        COALESCE(d.client_id, c_email.client_id)       AS client_id,
        d.company_id,
        d.incremental_id,
        d.erp_id,
        d.gifcard_code,
        d.created_at,
        d.created_at_chile,
        d.status,
        d.updated_at,
        d.subtotal_net,
        d.discount_amount,
        d.discount_pct,
        d.shipping_amount,
        d.courier,
        d.coupon_code,
        d.medio_pago,
        d.canal_compra,
        d.region_compra,
        d.ciudad_compra,
        d.bronze_id,
        d.ingested_at
    FROM deduped d
    LEFT JOIN {{ ref('dim_client') }} c_email
        ON d.tenant_id = c_email.tenant_id
        AND LOWER(d.customer_email) = LOWER(c_email.email)
        AND d.client_id IS NULL
        AND d.tenant_id = 'pf'
)

SELECT
    tenant_id,
    order_id,
    client_id,
    company_id,
    incremental_id,
    erp_id,
    gifcard_code,
    created_at,
    created_at_chile,
    status,
    updated_at,
    subtotal_net,
    discount_amount,
    discount_pct,
    shipping_amount,
    courier,
    coupon_code,
    medio_pago,
    canal_compra,
    region_compra,
    ciudad_compra,
    bronze_id,
    ingested_at
FROM with_company
