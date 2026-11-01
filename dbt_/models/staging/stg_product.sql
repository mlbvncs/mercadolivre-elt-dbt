{{ config(materialized='view') }}

WITH product_silver AS (
    SELECT *
    FROM {{ source('silver_S3', 'silver_data') }}
),

stg_product AS (
    SELECT
        COALESCE(NULLIF(sku, ''), 'Sem informação') AS sku,
        COALESCE(NULLIF(ean, ''), 'Sem informação') AS ean,
        COALESCE(NULLIF(image_url, ''), 'Sem informação') AS image_url,
        COALESCE(NULLIF(category_id, ''), 'Sem informação') AS category_id,
        COALESCE(NULLIF(domain_id, ''), 'Sem informação') AS domain_id,
        COALESCE(NULLIF(name, ''), 'Sem informação') AS name,
        COALESCE(NULLIF(condition, ''), 'Sem informação') AS condition
    FROM product_silver
),

product_deduplicate AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sku, ean, image_url, category_id, domain_id, name, condition
        ) AS product_rank
    FROM stg_product
)

SELECT 
    *
FROM product_deduplicate
WHERE product_rank = 1
