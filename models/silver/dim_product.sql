-- models/silver/dim_product.sql
-- Fuente: staging normalizado por tenant (ver models/staging/)
-- Tenant: pf, mc, aatn
-- Granularidad: una fila por producto
-- PK: tenant_id + sku

{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'sku'],
        incremental_strategy='merge',
        cluster_by=['tenant_id']
    )
}}

WITH
pf_raw AS (
    SELECT * FROM {{ ref('stg_pf__products') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

mc_raw AS (
    SELECT * FROM {{ ref('stg_mc__products') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

aatn_raw AS (
    SELECT * FROM {{ ref('stg_aatn__products') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

unioned AS (
    SELECT
        bronze_id, tenant_id, ingested_at,
        sku, sku_name, product_type,
        CAST(catalog_status AS STRING) AS catalog_status,
        brand, category_name, subcategory_name,
        CAST(created_at AS DATETIME) AS created_at,
        CAST(updated_at AS DATETIME) AS updated_at
    FROM pf_raw

    UNION ALL

    SELECT
        bronze_id, tenant_id, ingested_at,
        sku, sku_name, product_type,
        CAST(catalog_status AS STRING) AS catalog_status,
        brand, category_name, subcategory_name,
        CAST(created_at AS DATETIME) AS created_at,
        CAST(updated_at AS DATETIME) AS updated_at
    FROM mc_raw

    UNION ALL

    SELECT
        bronze_id, tenant_id, ingested_at,
        sku, sku_name, product_type,
        CAST(catalog_status AS STRING) AS catalog_status,
        brand, category_name, subcategory_name,
        CAST(created_at AS DATETIME) AS created_at,
        CAST(updated_at AS DATETIME) AS updated_at
    FROM aatn_raw
),

deduped AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_id, sku
                ORDER BY ingested_at DESC
            ) AS rn
        FROM unioned
    )
    WHERE rn = 1
)

SELECT
    tenant_id,
    sku,
    sku_name,
    product_type,
    catalog_status,
    brand,
    category_name,
    subcategory_name,
    created_at,
    updated_at,
    DATETIME(TIMESTAMP(created_at), 'America/Santiago') AS created_at_chile,
    bronze_id,
    ingested_at
FROM deduped
