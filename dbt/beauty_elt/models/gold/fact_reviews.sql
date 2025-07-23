{{ config(
    materialized='incremental',
    unique_key='review_uid',
    incremental_strategy='merge'
) }}

WITH unified_reviews AS (

  -- ✅ Oliveyoung
  SELECT
    CONCAT('oliveyoung_', review_id) AS review_uid,
    review_id,
    product_id,
    CAST(REGEXP_EXTRACT(star, r'(\d+)점$') AS INT64) AS star,
    review AS content,
    FORMAT_DATE('%Y.%m.%d', SAFE.PARSE_DATE('%Y.%m.%d', date)) AS created_at,
    category_name AS category,
    platform,
    scraped_at
  FROM `de6-2ez.silver.oliveyoung_reviews`

  UNION ALL

  -- ✅ Naver
  SELECT
    CONCAT('naver_', review_id) AS review_uid,
    review_id,
    product_id,
    SAFE_CAST(rating AS INT64) AS star,
    content,
    FORMAT_DATE('%Y.%m.%d', SAFE.PARSE_DATE('%y.%m.%d', created_at)) AS created_at,
    category AS category,
    platform,
    scraped_at
  FROM `de6-2ez.silver.naver_reviews`

  UNION ALL

  -- ✅ Musinsa
  SELECT
    CONCAT('musinsa_', review_id) AS review_uid,
    review_id,
    product_id,
    SAFE_CAST(grade AS INT64) AS star,
    content,
    FORMAT_DATE('%Y.%m.%d', DATE(createDate)) AS created_at,
    category_name AS category,
    platform,
    scraped_at
  FROM `de6-2ez.silver.musinsa_reviews`

)

SELECT *
FROM unified_reviews
WHERE 1=1
{% if is_incremental() %}
  AND scraped_at > (SELECT MAX(scraped_at) FROM {{ this }})
{% endif %}
