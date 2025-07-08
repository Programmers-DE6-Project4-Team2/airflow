from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.run import CloudRunJobRunOperator
from dags.common.oliveyoung_product_catagories import get_all_categories

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
    start_date=days_ago(1),
    catchup=False,
    tags=["oliveyoung", "cloud-run"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_cloud_run_job = CloudRunJobRunOperator.partial(
        task_id="run_cloud_run_job",
        job_name="oliveyoung-product-crawler-job",
        region="asia-northeast3",
        project_id="de6-2ez",
        poll_interval=30,
        timeout=900,
        wait_until_finished=True,
    ).expand(
        overrides=[
            {
                "args": [
                    category["category_name"],
                    category["category_url"],
                    str(category["max_pages"]),
                ]
            }
            for category in CATEGORY_LIST
        ]
    )

    start >> run_cloud_run_job >> end
