{{ config(
    materialized='external',
    location='s3://{{ env_var("S3_BUCKET_NAME") }}/gold/mercadolivre/iphone-18-pro-max/dim_product.parquet',
    format='parquet'
) }}

WITH product_stg AS (
    SELECT *
    FROM {{ ref('stg_product') }}
),

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'sku', 'ean', 'image_url', 'category_id', 'domain_id', 'name', 'condition'
    ]) }} AS product_key,
    *
FROM product_stg