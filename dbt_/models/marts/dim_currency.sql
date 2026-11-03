{{ config(
    materialized='external',
    location='s3://{{ env_var("S3_BUCKET_NAME") }}/gold/mercadolivre/iphone-18-pro-max/dim_currency.parquet',
    format='parquet'
) }}

WITH currency_stg AS (
    SELECT *
    FROM {{ ref('stg_currency') }}
),

SELECT
    {{ dbt_utils.generate_surrogate_key(['currency', 'currency_raw']) }} AS currency_key,
    *
FROM currency_stg