from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "myksphone2001@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="bronze_naver_review_fetch",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=default_args,
    description="Dynamic Task Mapping으로 Naver 리뷰 크롤링 수행",
    tags=["bronze", "naver", "reviews", "hourly"],
) as dag:

    @task()
    def get_target_products():
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        query = """
        SELECT 
            p.product_id,
            p.product_url,
            p.category_name,
            COALESCE(m.job_execute_count, 0) AS current_job_count
        FROM `de6-2ez.silver.naver_products` p
        LEFT JOIN `de6-2ez.metadata.review_crawling_metadata` m
            ON p.product_id = m.product_id AND m.platform = 'naver'
        WHERE m.product_id IS NULL OR m.job_execute_count < 5
        ORDER BY current_job_count ASC, p.product_id
        LIMIT 50
        """
        df = bq_hook.get_pandas_df(
            sql=query,
            dialect="standard",
            location="asia-northeast3",
        )
        logger.info("[get_target_products] 총 %d개 상품 반환됨", len(df))
        return df.to_dict("records")

    @task()
    def generate_overrides(products: list):
        overrides = []
        for product in products:
            overrides.append({
                "container_overrides": [
                    {
                        "env": [
                            {"name": "PRODUCT_ID", "value": product["product_id"]},
                            {"name": "PRODUCT_URL", "value": product["product_url"]},
                            {"name": "CATEGORY", "value": product["category_name"]},
                            {"name": "BUCKET_NAME", "value": "de6-ez2"},           # ✅ 순서 맞춤
                            {"name": "SORT_OPTION", "value": "랭킹순"},
                            {"name": "MAX_REVIEWS", "value": "100"},
                        ]
                    }
                ]
            })
        logger.info("[generate_overrides] %d개의 override 생성 완료", len(overrides))
        return overrides

    @task()
    def update_review_metadata(products: list):
        if not products:
            logger.warning("[update_review_metadata] 업데이트할 항목이 없습니다.")
            return

        bq_hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            location="asia-northeast3"
        )

        total_review_count = 100

        values = ",\n".join([
            f"""
            STRUCT(
                'naver' AS platform,
                '{p['product_id']}' AS product_id,
                {total_review_count} AS total_review_count,
                FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP()) AS execute_timestamp
            )
            """
            for p in products
        ])

        query = f"""
        MERGE `de6-2ez.metadata.review_crawling_metadata` AS target
        USING (
            SELECT * FROM UNNEST([
                {values}
            ])
        ) AS source
        ON target.platform = source.platform AND target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET
            job_execute_count = COALESCE(target.job_execute_count, 0) + 1,
            total_review_count = source.total_review_count,
            execute_timestamp = source.execute_timestamp
        WHEN NOT MATCHED THEN INSERT (
            platform, product_id, job_execute_count, total_review_count, execute_timestamp
        ) VALUES (
            source.platform, source.product_id, 1, source.total_review_count, source.execute_timestamp
        )
        """

        logger.info("[update_review_metadata] %d개 상품 메타데이터 업데이트 중...", len(products))
        bq_hook.insert_job(
            configuration={"query": {"query": query, "useLegacySql": False}},
            project_id="de6-2ez",
            location="asia-northeast3"
        )
        logger.info("[update_review_metadata] 업데이트 완료")

    # DAG Task 연결
    product_list = get_target_products()
    override_list = generate_overrides(product_list)

    run_review_crawling = CloudRunExecuteJobOperator.partial(
        task_id="run_review_crawling",
        project_id="de6-2ez",
        region="asia-northeast3",
        job_name="naver-review-job",
        gcp_conn_id="google_cloud_default",
        do_xcom_push=False,
    ).expand(overrides=override_list)

    metadata_update = update_review_metadata(product_list)

    product_list >> override_list >> run_review_crawling >> metadata_update
