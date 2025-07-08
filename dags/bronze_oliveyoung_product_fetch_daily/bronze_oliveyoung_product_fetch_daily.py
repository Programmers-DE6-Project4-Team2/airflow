from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from common.oliveyoung_product_catagories import get_all_categories
import pendulum

CATEGORY_DICT = get_all_categories()
CATEGORY_LIST = [
    {
        "category_name": name,
        "category_url": url,
        "max_pages": 5,
    }
    for name, url in CATEGORY_DICT.items()
]

with DAG(
    dag_id="bronze_oliveyoung_product_fetch_daily",
    schedule_interval="0 0 * * *",  # 매일 00:00 UTC → KST 오전 9시
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    catchup=False,
    tags=["oliveyoung", "cloud-run"],
    max_active_tasks=10,
    default_args={
        'owner': 'dawit0905@gmail.com',
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_cloud_run_job = CloudRunExecuteJobOperator.partial(
        task_id="run_cloud_run_job",
        project_id="de6-2ez",
        region="asia-northeast3",
        job_name="oliveyoung-product-crawler-job",
        do_xcom_push=False,  # XCom 사용 안 함으로 DB 연결 문제 방지
    ).task_concurrency(5).expand(
        overrides=[
            {
                "container_overrides": [
                    {
                        "env": [
                            {"name": "CATEGORY_NAME", "value": category["category_name"]},
                            {"name": "CATEGORY_URL", "value": category["category_url"]},
                            {"name": "MAX_PAGES", "value": str(category["max_pages"])},
                        ]
                    }
                ]
            }
            for category in CATEGORY_LIST
        ]
    )

    start >> run_cloud_run_job >> end
