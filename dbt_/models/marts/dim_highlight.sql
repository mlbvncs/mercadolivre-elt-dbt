{{ config(
    materialized='external',
    location='s3://mlbvncs-valuemetrics-dev/gold/mercadolivre/iphone-18-pro-max/dim_highlight.parquet',
    format='parquet'
) }}

WITH highlight_stg AS (
    SELECT *
    FROM {{ ref('stg_highlight') }}
),

SELECT
    {{ dbt_utils.generate_surrogate_key(['highlight']) }} AS highlight_key,
    *
FROM highlight_stg