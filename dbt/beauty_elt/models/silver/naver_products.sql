{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY scraped_at DESC) AS row_num
    FROM {{ source('bronze', 'naver_products') }}
    WHERE category_name IS NOT NULL AND category_name != '전체'
)

SELECT
    CAST(product_id AS STRING) AS product_id,
    name,
    brand,
    price,
    representativeImageUrl,
    reviewCount,
    avgReviewScore,
    product_url,
    categories,
    category_name,
    channelName,
    scraped_at,
    '네이버' AS platform 
FROM ranked
WHERE row_num = 1