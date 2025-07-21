# dags/bronze_musinsa_product_fetch_daily.py
import pendulum

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from common.musinsa_product_categories import CATEGORY_MAPPING

# 기본 설정
default_args = {
    'owner': 'tjalwled12@gmail.com',
    'start_date': pendulum.datetime(2025, 7, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# DAG 정의
dag = DAG(
    'bronze_musinsa_product_crawler_daily',
    default_args=default_args,
    description='무신사 상품 크롤링',
    schedule_interval="0 0 * * *",  # 매일 00:00 UTC → KST 오전 9시
    catchup=False,
    max_active_runs=1,
    tags=['musinsa', 'cloud-run']
)

PROJECT_ID = "de6-2ez"
REGION = "asia-northeast3"
JOB_NAME = "musinsa-product-job"

# 카테고리별 환경변수 리스트 생성
category_env_list = []
for category_code in CATEGORY_MAPPING.keys():
    env_vars = [
        {'name': 'ENV', 'value': 'production'},
        {'name': 'CATEGORY_CODE', 'value': category_code},
        {'name': 'GCS_PROJECT_ID', 'value': PROJECT_ID},
        {'name': 'MAX_PAGES', 'value': '8'},
        {'name': 'LOG_LEVEL', 'value': 'INFO'},
        {'name': 'REQUEST_DELAY', 'value': '1.0'},
    ]
    category_env_list.append({
        'container_overrides': [{'env': env_vars}]
    })

# partial + expand로 동적 태스크 생성
run_product_jobs = CloudRunExecuteJobOperator.partial(
    task_id='run_product_jobs',
    project_id=PROJECT_ID,
    region=REGION,
    job_name=JOB_NAME,
    do_xcom_push=False,  # XCom 사용 안 함으로 DB 연결 문제 방지
    dag=dag
).expand(
    overrides=category_env_list
)
