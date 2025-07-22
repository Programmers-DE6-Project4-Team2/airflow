from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from airflow.utils.dates import days_ago

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
    description="Load OliveYoung product CSVs from GCS to BigQuery Bronze in a single batch",
    tags=["bronze", "oliveyoung", "products"],
) as dag:

    def load_csvs_to_bq(**context):
        execution_date = context["execution_date"].in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = "raw-data/olive-young/products/"
        search_prefix = f"{prefix}{year}/{month}/{day}/"  # ✅ 정확한 경로만 조회

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")

        # 해당 날짜의 CSV 목록만 조회
        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=search_prefix)
        file_list = [blob for blob in blobs if blob.endswith(".csv")]

        if not file_list:
            print(f"[oliveyoung_products] ❗ No files found for {year}-{month}-{day}")
            return

        source_uris = [f"gs://{bucket_name}/{blob}" for blob in file_list]
        print(f"[oliveyoung_products] ✅ Found {len(source_uris)} file(s) to load")

        # BigQuery LoadJob 설정
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
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

        # 한 번의 insert_job으로 모든 파일 적재
        bq_hook.insert_job(
            configuration={
                "load": {
                    "sourceUris": source_uris,
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

        print(f"[oliveyoung_products] ✅ Loaded {len(source_uris)} files to BigQuery")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
        provide_context=True,
    )

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_oliveyoung_dbt",
        trigger_dag_id="silver_oliveyoung_product_dbt",
        wait_for_completion=False,
    )

    load_task >> trigger_silver_dag
