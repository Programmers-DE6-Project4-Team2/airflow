from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum

default_args = {
    "owner": "dawit0905@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="bronze_oliveyoung_review_fetch",
        start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
        schedule_interval="0 * * * *",  # 매시 정각 실행
        catchup=False,
        default_args=default_args,
        description="OliveYoung 리뷰 크롤링 대상 상품 선별 및 처리",
        tags=["bronze", "oliveyoung", "reviews", "hourly"],
) as dag:
    def get_target_products(**context):
        """
        silver.oliveyoung_products와 metadata.review_crawling_metadata를 LEFT JOIN하여
        리뷰 크롤링이 필요한 상품들을 선별하는 함수
        """
        bq_hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            location="asia-northeast3"
        )

        query = """
        SELECT 
            p.product_id,
            p.name,
            p.product_url,
            p.brand,
            COALESCE(m.job_execute_count, 0) as current_job_count
        FROM `de6-2ez.silver.oliveyoung_products` p
        LEFT JOIN `de6-2ez.metadata.review_crawling_metadata` m
            ON p.product_id = m.product_id
        WHERE m.product_id IS NULL 
            OR m.job_execute_count < 5
        ORDER BY COALESCE(m.job_execute_count, 0) ASC, p.product_id
        LIMIT 100
        """

        print(f"[bronze_oliveyoung_review_fetch] 대상 상품 선별 쿼리 실행 중...")

        # BigQuery에서 결과 조회
        result = bq_hook.get_pandas_df(
            sql=query,
            dialect="standard"
        )

        print(f"[bronze_oliveyoung_review_fetch] 총 {len(result)} 개 상품이 리뷰 크롤링 대상으로 선별됨")

        # 결과를 그룹별로 분류하여 로깅
        if len(result) > 0:
            new_products = result[result['current_job_count'] == 0]
            retry_products = result[result['current_job_count'] > 0]

            print(f"  - 신규 상품 (job_execute_count = 0): {len(new_products)}개")
            print(f"  - 재시도 상품 (job_execute_count < 5): {len(retry_products)}개")

            # 상위 10개 상품 정보 출력
            print("\n[상위 10개 대상 상품 정보]")
            for idx, row in result.head(10).iterrows():
                print(f"  {idx + 1}. {row['name']} (브랜드: {row['brand']}, job_execute_count: {row['current_job_count']})")

            # XCom에 결과 저장 (향후 실제 크롤링 작업에서 사용)
            context['task_instance'].xcom_push(
                key='target_products',
                value=result.to_dict('records')
            )

        else:
            print("[bronze_oliveyoung_review_fetch] 리뷰 크롤링 대상 상품이 없습니다.")

        return len(result)


    # Task 정의
    get_target_products_task = PythonOperator(
        task_id="get_target_products",
        python_callable=get_target_products,
        provide_context=True,
    )

    get_target_products_task
