{{ config(materialized='view') }}

WITH highlight_silver AS (
    SELECT *
    FROM {{ source('silver_S3', 'silver_data') }}
),

stg_highlight AS (
    SELECT
        COALESCE(NULLIF(highlight, ''), 'Sem destaque') AS highlight
    FROM highlight_silver
),

highlight_deduplicate AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY highlight
        ) AS highlight_rank
    FROM stg_highlight
)

SELECT
    *
FROM highlight_deduplicate
WHERE highlight_rank = 1