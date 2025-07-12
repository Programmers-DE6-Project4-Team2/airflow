from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery
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
        bucket_name = "bronze-layer-example"
        bucket = client.bucket(bucket_name)
        prefix = "musinsa/products/"

        # 지정 경로의 모든 CSV 파일 목록 가져오기
        blobs = list(bucket.list_blobs(prefix=prefix))
        file_list = [b.name for b in blobs if b.name.endswith(".csv")]

        # BigQuery 대상 테이블
        table_id = "final-project-practice-465301.bronze.musinsa_products"

        # BigQuery 적재 설정
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # 헤더 존재 시
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # GCS URI 기반으로 적재 실행
        for blob_name in file_list:
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            load_job = bq_client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
            )
            load_job.result()  # 완료 대기
            print(f"[musinsa] Loaded file: {gcs_uri}")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
    )

