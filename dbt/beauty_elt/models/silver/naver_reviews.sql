{{ config(
    materialized='incremental',
    unique_key='review_id',
    incremental_strategy='merge'
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY scraped_at DESC
        ) AS row_num
    FROM {{ source('bronze', 'naver_reviews') }}
)

SELECT
    CAST(review_id AS STRING) AS review_id,
    CAST(product_id AS STRING) AS product_id,
    username,
    created_at,
    scraped_at,
    rating,
    content,
    option,
    category,
    sort_option,
    '네이버' AS platform

FROM ranked
WHERE row_num = 1
