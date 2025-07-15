from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import pendulum

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_naver_products",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=True,
    default_args=default_args,
    description="Load Naver Beauty product CSVs from GCS to BigQuery Bronze",
    tags=["bronze", "naver", "products"],
) as dag:
    
    def load_csvs_to_bq(**context):
        execution_date = context["execution_date"].in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = "raw-data/naver/products/"
        search_prefix = f"{prefix}"

        # Hook 사용
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")

        # GCS에서 대상 CSV 목록 가져오기
        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=search_prefix)
        file_list = [
            blob for blob in blobs
            if blob.endswith(".csv") and f"/{year}/{month}/{day}/" in blob
        ]

        print(f"[naver_products] Found {len(file_list)} file(s) for {year}-{month}-{day}:")

        table_id = "de6-2ez.bronze.naver_products"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
            autodetect=True,
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
                            "tableId": "naver_products"
                        },
                        "writeDisposition": job_config.write_disposition,
                        "skipLeadingRows": job_config.skip_leading_rows,
                        "sourceFormat": job_config.source_format,
                        "allowQuotedNewlines": job_config.allow_quoted_newlines,
                        "autodetect": job_config.autodetect
                    }
                },
                project_id="de6-2ez"
            )
            print(f"[naver_products] Loaded file: {gcs_uri}")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
        provide_context=True,
    )

