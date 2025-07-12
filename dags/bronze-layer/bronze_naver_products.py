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
    dag_id="bronze_naver_products",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Load Naver Beauty product CSVs from GCS to BigQuery Bronze",
    tags=["bronze", "naver", "products"],
) as dag:

    def load_csvs_to_bq(**context):
        client = storage.Client()
        bq_client = bigquery.Client()

        bucket = client.bucket("bronze-layer-example")
        prefix = "naver/products/"  

        blobs = list(bucket.list_blobs(prefix=prefix))
        file_list = [b.name for b in blobs if b.name.endswith(".csv")]

        print(f"[naver] Found {len(file_list)} file(s):")
        for file_name in file_list:
            blob = bucket.blob(file_name)
            content = blob.download_as_text(encoding="utf-8")
            df = pd.read_csv(io.StringIO(content))

            table_id = "final-project-practice-465301.bronze.naver_products"  # 네이버용 테이블 ID
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True,
            )

            bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
            print(f"[naver] Loaded {len(df)} rows from {file_name} to Bronze.")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
    )
