from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_musinsa_products",
    start_date=days_ago(1),
    schedule_interval="0 4 * * *",  # 2시 실행
    catchup=True,
    default_args=default_args,
    description="Load Musinsa review CSVs from GCS to BigQuery Bronze in a single batch",
    tags=["bronze", "musinsa", "reviews"],
    max_active_tasks=1,
) as dag:

    @task
    def list_product_csv_files(execution_date=None):
        execution_date = execution_date.in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = f"raw-data/musinsa/reviews/"
        target_path = f"{prefix}{year}/{month}/{day}/"

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=target_path)

        file_list = [b for b in blobs if b.endswith(".csv")]
        print(f"✅ Found {len(file_list)} MUSINSA review files under {target_path}")
        return file_list

    @task
    def load_csvs_to_bq(blob_list: list[str]):
        uris = [f"gs://de6-ez2/{blob}" for blob in blob_list]
        print(f"📦 Loading {len(uris)} files to BigQuery...")

        bq_client = bigquery.Client(project="de6-2ez")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
            autodetect=True,
        )

        load_job = bq_client.load_table_from_uri(
            source_uris=uris,
            destination="de6-2ez.bronze.musinsa_reviews",
            job_config=job_config,
        )
        load_job.result()
        print(f"✅ Successfully loaded {len(uris)} files into BigQuery")

    file_list = list_product_csv_files()
    load_csvs_to_bq(file_list)

