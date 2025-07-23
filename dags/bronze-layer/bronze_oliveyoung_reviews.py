from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_oliveyoung_reviews",
    start_date=days_ago(1),
    schedule_interval="30 14 * * *", # 23시 30분 실행
    catchup=True,
    default_args=default_args,
    description="Load OliveYoung review CSVs from GCS to BigQuery Bronze in a single batch",
    tags=["bronze", "oliveyoung", "reviews"],
    max_active_tasks=1,  # 한 번에 하나의 배치 작업만 실행
) as dag:

    @task
    def list_review_csv_files(execution_date=None):
        """GCS에서 해당 날짜의 리뷰 CSV 파일 경로 리스트업"""
        execution_date = execution_date.in_timezone("Asia/Seoul")
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")

        bucket_name = "de6-ez2"
        prefix = f"raw-data/olive-young/reviews/{year}/{month}/{day}/"

        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=prefix)

        file_list = [b for b in blobs if b.endswith(".csv")]
        print(f"✅ Found {len(file_list)} review files under {prefix}")
        return file_list

    @task
    def load_csvs_to_bq(blob_list: list[str]):
        """여러 리뷰 CSV 파일을 한 번에 BigQuery에 적재"""
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
            destination="de6-2ez.bronze.oliveyoung_reviews",
            job_config=job_config,
        )
        load_job.result()
        print(f"✅ Successfully loaded {len(uris)} files into BigQuery")

    file_list = list_review_csv_files()
    load_task = load_csvs_to_bq(file_list)

    # dbt trigger 예: 나중에 사용하려면 여기에 연결
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_silver_oliveyoung_review_dbt",
        trigger_dag_id="silver_oliveyoung_review_dbt",
        wait_for_completion=False,
    )

    load_task >> trigger_dbt
