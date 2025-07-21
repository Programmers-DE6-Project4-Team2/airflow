from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from airflow.utils.dates import days_ago

import time
import random

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_oliveyoung_products",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
    catchup=True,  # backfill 가능하도록 설정
    default_args=default_args,
    description="Load OliveYoung product CSVs from GCS to BigQuery Bronze",
    tags=["bronze", "oliveyoung", "products"],
) as dag:

    def load_csvs_to_bq(**context):

        execution_date = context["execution_date"].in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = "raw-data/olive-young/products/"
        search_prefix = f"{prefix}"
        
        # Hook 기반 클라이언트 생성 (Airflow Connection 사용)
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")

        # GCS 파일 목록 가져오기
        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=search_prefix)
        file_list = [
            blob for blob in blobs
            if blob.endswith(".csv") and f"/{year}/{month}/{day}/" in blob
        ]

        print(f"[oliveyoung_products] Found {len(file_list)} file(s) for {year}-{month}-{day}:")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,  # 줄바꿈 포함된 리뷰도 처리
            autodetect=False,
            schema=[
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("rating", "FLOAT"),
                bigquery.SchemaField("review_count", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP"),
            ]
        )

        for blob_name in file_list:
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            bq_hook.insert_job(
                configuration={
                    "load": {
                        "sourceUris": [gcs_uri],
                        "destinationTable": {
                            "projectId": "de6-2ez",
                            "datasetId": "bronze",
                            "tableId": "oliveyoung_products"
                        },
                        "writeDisposition": job_config.write_disposition,
                        "skipLeadingRows": job_config.skip_leading_rows,
                        "sourceFormat": job_config.source_format,
                        "allowQuotedNewlines": job_config.allow_quoted_newlines,
                        "schema": {
                            "fields": [field.to_api_repr() for field in job_config.schema]
                        }
                    }
                },
                project_id="de6-2ez"
            )
            print(f"[oliveyoung_products] Loaded file: {gcs_uri}")
            time.sleep(random.uniform(1.5, 2.5))

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
        provide_context=True,
    )

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_oliveyoung_dbt",
        trigger_dag_id="silver_oliveyoung_product_dbt",  # 이 DAG ID는 dbt 실행용 DAG에서 설정한 값과 일치해야 함
        wait_for_completion=False,
    )

    load_task >> trigger_silver_dag