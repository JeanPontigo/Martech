-- models/gold/client_type_daily.sql
-- Fuente: silver.fact_orders
-- Granularidad: una fila por cliente por dia
-- Clasifica clientes como Nuevo o Recurrente por dia y por mes

{{
    config(
        materialized='table',
        schema='gold',
        cluster_by=['tenant_id']
    )
}}

WITH

-- -----------------------------------------------------------------------
-- 1. Primera compra por cliente
-- -----------------------------------------------------------------------
first_purchase AS (
    SELECT
        tenant_id,
        client_id,
        MIN(DATE(created_at))                           AS first_purchase_date,
        DATE_TRUNC(MIN(DATE(created_at)), MONTH)        AS first_purchase_month
    FROM {{ ref('fact_orders') }}
    GROUP BY tenant_id, client_id
),

-- -----------------------------------------------------------------------
-- 2. Clasificar clientes por dia y por mes
-- -----------------------------------------------------------------------
clientes_por_dia AS (
    SELECT
        o.tenant_id,
        DATE(o.created_at)                              AS fecha,
        o.client_id,
        CASE
            WHEN DATE_TRUNC(DATE(o.created_at), MONTH) = fp.first_purchase_month
            THEN 'Nuevo'
            ELSE 'Recurrente'
        END                                             AS tipo_cliente_mes,
        CASE
            WHEN DATE(o.created_at) = fp.first_purchase_date
            THEN 'Nuevo'
            ELSE 'Recurrente'
        END                                             AS tipo_cliente_dia
    FROM {{ ref('fact_orders') }} o
    JOIN first_purchase fp
        ON o.client_id = fp.client_id
        AND o.tenant_id = fp.tenant_id
    WHERE DATE(o.created_at) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 11 MONTH), MONTH)
    AND DATE(o.created_at) <= LAST_DAY(CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.tenant_id, o.client_id, DATE(o.created_at)
        ORDER BY CASE WHEN DATE(o.created_at) = fp.first_purchase_date THEN 0 ELSE 1 END
    ) = 1
)

-- -----------------------------------------------------------------------
-- 3. Output final
-- -----------------------------------------------------------------------
SELECT
    tenant_id,
    fecha,
    client_id,
    tipo_cliente_mes,
    tipo_cliente_dia,
    FIRST_VALUE(tipo_cliente_dia) OVER (
        PARTITION BY tenant_id, client_id, DATE_TRUNC(fecha, MONTH)
        ORDER BY fecha ASC
    ) AS tipo_cliente_mes_calculado
FROM clientes_por_dia
