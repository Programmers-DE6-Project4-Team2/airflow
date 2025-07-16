version: 2

models:
    - name: naver_products    # dbt 모델 파일 이름 (naver_products.sql)
        description: "네이버 뷰티 상품 데이터 (silver)"
        columns:
            - name: product_id
                description: "상품 고유 ID"
                tests:
                    - unique
                    - not_null
            - name: brand
                description: "브랜드명"
