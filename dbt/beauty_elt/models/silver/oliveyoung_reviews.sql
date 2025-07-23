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
    FROM {{ source('bronze', 'oliveyoung_reviews') }}
)

SELECT
    CAST(review_id AS STRING) AS review_id,
    product_name,
    star,
    review,
    skin_type,
    date,
    purchase_type,
    page,
    helpful,
    scraped_at,
    total_review_count,
    product_id,
    product_url,
    category_name,
    crawling_timestamp,
    crawling_date,
    source,
    data_type,
    '올리브영' AS platform

FROM ranked
WHERE row_num = 1