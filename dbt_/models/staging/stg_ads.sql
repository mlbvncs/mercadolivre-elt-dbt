{{ config(materialized='view') }}

WITH silver AS (
    SELECT *
    FROM {{ source('silver_S3', 'silver_data') }}
),

SELECT
    COALESCE(NULLIF(sku, ''), 'Sem informação') AS sku,
    COALESCE(NULLIF(ean, ''), 'Sem informação') AS ean,
    COALESCE(NULLIF(image_url, ''), 'Sem informação') AS image_url,
    COALESCE(NULLIF(category_id, ''), 'Sem informação') AS category_id,
    COALESCE(NULLIF(domain_id, ''), 'Sem informação') AS domain_id,
    COALESCE(NULLIF(name, ''), 'Sem informação') AS name,
    COALESCE(NULLIF(condition, ''), 'Sem informação') AS condition,
    COALESCE(NULLIF(seller_id, ''), 'Sem informação') AS seller_id,
    COALESCE(NULLIF(seller_name, ''), 'Sem informação') AS seller_name,
    COALESCE(NULLIF(seller_city, ''), 'Sem informação') AS seller_city,
    COALESCE(NULLIF(seller_state, ''), 'Sem informação') AS seller_state,
    COALESCE(NULLIF(seller_country, ''), 'Sem informação') AS seller_country,
    CASE
        WHEN is_best_seller IS NULL THEN 'Sem informação'
        WHEN is_best_seller THEN 'Verdadeiro'
        ELSE 'Falso'
    END AS is_best_seller,
    CASE
        WHEN is_power_seller IS NULL THEN 'Sem informação'
        WHEN is_power_seller THEN 'Verdadeiro'
        ELSE 'Falso'
    END AS is_power_seller,
    COALESCE(NULLIF(power_seller_status_title, ''), 'Sem informação') AS power_seller_status_title,
    COALESCE(NULLIF(highlight, ''), 'Sem destaque') AS highlight,
    COALESCE(NULLIF(currency, ''), 'Sem informação') AS currency,
    COALESCE(NULLIF(currency_raw, ''), 'Sem informação') AS currency_raw,
    price,
    rating,
    review_count,
    url,
    COALESCE(NULLIF(extracted_at, ''), 'Sem informação') AS extracted_at
FROM silver