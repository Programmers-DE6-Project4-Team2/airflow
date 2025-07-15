from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
from airflow.utils.dates import days_ago
import pendulum

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_oliveyoung_products",
    start_date=days_ago(1),
    schedule_interval="@daily",
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

        # 클라이언트 생성
        client = storage.Client()
        bq_client = bigquery.Client()

        # 해당 날짜에 해당하는 GCS 파일 목록 필터링
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=search_prefix))
        file_list = [
            b.name for b in blobs
            if b.name.endswith(".csv") and f"/{year}/{month}/{day}/" in b.name
        ]

        print(f"[oliveyoung_products] Found {len(file_list)} file(s) for {year}-{month}-{day}:")

        table_id = "de6-2ez.bronze.oliveyoung_products"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,  # 줄바꿈 포함된 리뷰도 처리
            autodetect=True,
            schema=[
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("rating", "FLOAT"),
                bigquery.SchemaField("review_count", "STRING"),  # 고정!
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP"),
            ]
        )

        for blob_name in file_list:
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config).result()
            print(f"[oliveyoung_products] Loaded file: {gcs_uri}")

    load_task = PythonOperator(
        task_id="load_csvs_to_bq",
        python_callable=load_csvs_to_bq,
        provide_context=True,
    )
