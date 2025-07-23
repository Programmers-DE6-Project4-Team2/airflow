{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY scraped_at DESC) AS row_num
    FROM {{ source('bronze', 'musinsa_products') }}
    WHERE category_name IS NOT NULL
)

SELECT
    CAST(product_id AS STRING) AS product_id,
    name,
    brand,
    price,
    original_price,
    discount_rate,
    rating,
    review_count,
    CAST(likes AS STRING) AS likes,
    image_url,
    product_url,
    number_of_views,
    sales,
    scraped_at,
    category_name,
    category_code,
    '무신사' AS platform
FROM ranked
WHERE row_num = 1