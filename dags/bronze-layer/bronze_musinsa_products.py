from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_musinsa_products",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
    catchup=True,
    default_args=default_args,
    description="Load Musinsa product CSVs from GCS to BigQuery Bronze (batch insert)",
    tags=["bronze", "musinsa", "products"],
) as dag:

    def load_csvs_to_bq(**context):
        execution_date = context["execution_date"].in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = "raw-data/musinsa/products/"
        search_prefix = f"{prefix}{year}/{month}/{day}/"  # 날짜 디렉토리만 탐색

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")

        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=search_prefix)
        file_list = [blob for blob in blobs if blob.endswith(".csv")]

        if not file_list:
            print(f"[musinsa_products] ❗ No CSV files found under {search_prefix}")
            return

        source_uris = [f"gs://{bucket_name}/{blob}" for blob in file_list]
        print(f"[musinsa_products] ✅ Found {len(source_uris)} file(s) to load")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
            autodetect=False,
            schema=[
                bigquery.SchemaField("rank", "INTEGER"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("original_price", "INTEGER"),
                bigquery.SchemaField("discount_rate", "INTEGER"),
                bigquery.SchemaField("rating", "INTEGER"),
                bigquery.SchemaField("review_count", "INTEGER"),
                bigquery.SchemaField("likes", "STRING"),
                bigquery.SchemaField("image_url", "STRING"),
                bigquery.SchemaField("product_url", "STRING"),
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("number_of_views", "INTEGER"),
                bigquery.SchemaField("sales", "INTEGER"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP"),
                bigquery.SchemaField("category_name", "STRING"),
                bigquery.SchemaField("category_code", "INTEGER"),
            ]
        )

        # ✅ 한 번의 insert_job으로 모든 파일 적재
        bq_hook.insert_job(
            configuration={
                "load": {
                    "sourceUris": source_uris,
                    "destinationTable": {
                        "projectId": "de6-2ez",
                        "datasetId": "bronze",
                        "tableId": "musinsa_products"
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

        print(f"[musinsa_products] ✅ Loaded {len(source_uris)} files to BigQuery")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
        provide_context=True,
    )

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_musinsa_dbt",
        trigger_dag_id="silver_musinsa_product_dbt",
        wait_for_completion=False,
    )

    load_task >> trigger_silver_dag
