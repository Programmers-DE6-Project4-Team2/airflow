{{ config(
    materialized='incremental',
    unique_key='product_uid',
    incremental_strategy='merge'
) }}

WITH unified_products AS (

  -- ✅ Naver
  SELECT
    CONCAT('naver_', product_id) AS product_uid,
    product_id,
    name,
    brand,
    price,
    ROUND(avgReviewScore, 1) AS rating,
    SAFE_CAST(reviewCount AS INT64) AS review_count,
    category_name AS category,
    platform,
    scraped_at
  FROM `de6-2ez.silver.naver_products`

  UNION ALL

  -- ✅ Oliveyoung (문자열 리뷰수 처리)
  SELECT
    CONCAT('oliveyoung_', product_id) AS product_uid,
    product_id,
    name,
    brand,
    price,
    ROUND(rating, 1) AS rating,
    SAFE_CAST(REGEXP_REPLACE(CAST(review_count AS STRING), r"[^\d]", "") AS INT64) AS review_count,
    category,
    platform,
    scraped_at
  FROM `de6-2ez.silver.oliveyoung_products`

  UNION ALL

  -- ✅ Musinsa (별점 0~100 → 0~5, 정수 리뷰 수)
  SELECT
    CONCAT('musinsa_', product_id) AS product_uid,
    product_id,
    name,
    brand,
    price,
    ROUND(SAFE_DIVIDE(SAFE_CAST(rating AS FLOAT64), 20.0), 1) AS rating,
    SAFE_CAST(review_count AS INT64) AS review_count,
    category_name AS category,
    platform,
    scraped_at
  FROM `de6-2ez.silver.musinsa_products`

)

SELECT *
FROM unified_products

{% if is_incremental() %}
WHERE scraped_at > (SELECT MAX(scraped_at) FROM {{ this }})
{% endif %}
