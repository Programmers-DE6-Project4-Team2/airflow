{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY scraped_at DESC) AS row_num
  FROM {{ source('bronze', 'naver_products') }}
)

SELECT
    product_id,
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
    scraped_at
FROM ranked
WHERE row_num = 1