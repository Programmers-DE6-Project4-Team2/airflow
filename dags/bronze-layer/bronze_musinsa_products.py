from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
from datetime import timedelta
import pendulum

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_musinsa_products",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=True,
    default_args=default_args,
    description="Load Musinsa product CSVs from GCS to BigQuery Bronze",
    tags=["bronze", "musinsa", "products"],
) as dag:

    def load_csvs_to_bq(**context):
        execution_date = context["execution_date"].in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = "raw-data/musinsa/products/"
        search_prefix = f"{prefix}"

        client = storage.Client()
        bq_client = bigquery.Client()

        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=search_prefix))
        file_list = [
            b.name for b in blobs
            if b.name.endswith(".csv") and f"/{year}/{month}/{day}/" in b.name
        ]

        print(f"[musinsa_products] Found {len(file_list)} file(s) for {year}-{month}-{day}:")

        table_id = "de6-2ez.bronze.musinsa_products"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
            autodetect=True,
        )

        for blob_name in file_list:
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config).result()
            print(f"[musinsa_products] Loaded file: {gcs_uri}")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
        provide_context=True,
    )
    