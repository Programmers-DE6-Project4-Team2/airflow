{{ config(
    materialized='incremental',
    unique_key='product_uid',
    incremental_strategy='merge'
) }}

-- ✅ Naver
SELECT
  CONCAT('naver_', product_id) AS product_uid,
  product_id,
  name,
  brand,
  price,
  ROUND(avgReviewScore, 1) AS rating,
  CAST(reviewCount AS INT64) AS review_count,
  category_name AS category,
  platform,
  scraped_at
FROM `de6-2ez.silver.naver_products`

UNION ALL

-- ✅ Oliveyoung
SELECT
  CONCAT('oliveyoung_', product_id) AS product_uid,
  product_id,
  name,
  brand,
  price,
  ROUND(rating, 1) AS rating,
  CAST(review_count AS INT64) AS review_count,
  category,
  platform,
  scraped_at
FROM `de6-2ez.silver.oliveyoung_products`

UNION ALL

-- ✅ Musinsa
SELECT
  CONCAT('musinsa_', product_id) AS product_uid,
  product_id,
  name,
  brand,
  price,
  ROUND(SAFE_DIVIDE(CAST(rating AS FLOAT64), 20.0), 1) AS rating,
  CAST(review_count AS INT64) AS review_count,
  category_name AS category,
  platform,
  scraped_at
FROM `de6-2ez.silver.musinsa_products`
