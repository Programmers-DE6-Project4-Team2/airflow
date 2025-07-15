from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
import pendulum
from common.naver_product_categories import get_all_product_categories

CATEGORY_LIST = get_all_product_categories()
BUCKET_NAME = "de6-ez2"

with DAG(
    dag_id="bronze_naver_product_fetch_daily",
    schedule_interval="0 0 * * *",  # 매일 00:00 UTC → 오전 9시 KST
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    catchup=False,
    tags=["naver", "product", "cloud-run"],
    default_args={
        "owner": "myksphone2001@gmail.com",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_product_jobs = CloudRunExecuteJobOperator.partial(
        task_id="run_naver_product_crawler",
        project_id="de6-2ez",
        region="asia-northeast3",
        job_name="naver-product-job",
        gcp_conn_id="google_cloud_default",
        do_xcom_push=False,
    ).expand(
        overrides=[
            {
                "container_overrides": [
                    {
                        "env": [
                            {"name": "CATEGORY", "value": category},
                            {"name": "BUCKET_NAME", "value": BUCKET_NAME},
                        ]
                    }
                ]
            }
            for category in CATEGORY_LIST
        ]
    )

    start >> run_product_jobs >> end
