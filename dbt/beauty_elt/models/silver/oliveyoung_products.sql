{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY scraped_at DESC) AS row_num
    FROM {{ source('bronze', 'oliveyoung_products') }}
)

SELECT
    product_id,
    name,
    brand,
    price,
    url AS product_url,
    rating,
    review_count,
    category,
    scraped_at,
    '올리브영' AS platform 
FROM ranked
WHERE row_num = 1