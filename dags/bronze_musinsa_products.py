from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery
import pandas as pd
import io
from datetime import timedelta

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_musinsa_products",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Load Musinsa product CSVs from GCS to BigQuery Bronze",
    tags=["bronze", "musinsa", "products"],
) as dag:

    def load_csvs_to_bq(**context):

        # GCS 및 BigQuery 클라이언트 생성
        client = storage.Client()
        bq_client = bigquery.Client()
        
        # GCS 버킷과 경로 설정
        bucket = client.bucket("bronze-layer-example")
        prefix = "musinsa/products/"

        # 지정 경로의 모든 CSV 파일 목록 가져오기
        blobs = list(bucket.list_blobs(prefix=prefix))
        file_list = [b.name for b in blobs if b.name.endswith(".csv")]

        print(f"[musinsa] Found {len(file_list)} file(s):")
        for file_name in file_list:
            # CSV 파일 내용을 읽어 pandas DataFrame으로 변환
            blob = bucket.blob(file_name)
            content = blob.download_as_text(encoding="utf-8")
            df = pd.read_csv(io.StringIO(content))
            
            table_id = "final-project-practice-465301.bronze.musinsa_products"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",  # 기존 데이터 유지하며 추가
                autodetect=True,
            )

            # DataFrame → BigQuery 적재 실행 및 완료 대기
            bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
            print(f"[musinsa] Loaded {len(df)} rows from {file_name} to Bronze.")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
    )

