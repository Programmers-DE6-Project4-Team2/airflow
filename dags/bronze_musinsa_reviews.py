from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery
import pandas as pd
import io
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_musinsa_reviews",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Load Musinsa review CSVs from GCS to BigQuery Bronze",
    tags=["bronze", "musinsa", "reviews"],
) as dag:

    def load_csvs_to_bq(**context):
        # GCS 및 BigQuery 클라이언트 생성
        gcs_client = storage.Client()
        bq_client = bigquery.Client()

        # GCS 버킷 및 경로
        bucket = gcs_client.bucket("bronze-layer-example")
        prefix = "musinsa/reviews/"

        # 리뷰 CSV 파일 목록 가져오기
        blobs = list(bucket.list_blobs(prefix=prefix))
        file_list = [b.name for b in blobs if b.name.endswith(".csv")]

        print(f"[musinsa_reviews] Found {len(file_list)} file(s):")
        for file_name in file_list:
            blob = bucket.blob(file_name)
            content = blob.download_as_text(encoding="utf-8")
            df = pd.read_csv(io.StringIO(content))
            
            # 점(.) 제거 및 소문자 처리 → BigQuery 컬럼명 제약 해결
            df.columns = [col.lower().replace(".", "_") for col in df.columns]

            # BigQuery 테이블 ID (dataset: bronze)
            table_id = "final-project-practice-465301.bronze.musinsa_reviews"

            # BigQuery 적재 설정
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",  # 기존 데이터 유지 + 추가
                autodetect=True,  # 스키마 자동 감지
            )

            # 데이터 적재
            bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
            print(f"[musinsa_reviews] Loaded {len(df)} rows from {file_name} to {table_id}")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
    )
