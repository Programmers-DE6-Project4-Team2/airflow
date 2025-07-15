from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery
import pandas as pd
import io
import json
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver_oliveyoung_products",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="ELT for OliveYoung products: Bronze(GCS) → Silver(BigQuery)",
    tags=["bronze", "silver", "oliveyoung", "products"],
) as dag:

    # -------------------------------
    # Task 1: GCS에서 CSV 파일 목록 가져오기
    # -------------------------------
    def list_olive_files(**context):
        #GCP의 Cloud Stroage 클라이언트를 생성(환경 변수 GOOGLE_APPLICATION_CREDENTIALS 참조)
        client = storage.Client()
        bucket = client.bucket("bronze-layer-example")
        prefix = "oliveyoung/products/"

        # 지정한 경로(prefix)아래의 모든 객체(blob)를 가져옴
        blobs = list(bucket.list_blobs(prefix=prefix))
        file_list = [blob.name for blob in blobs if blob.name.endswith(".csv")]

        # 찾은 파일 수와 이름을 출력 (로그 확인용)
        print(f"[oliveyoung] Found {len(file_list)} file(s):")
        for f in file_list:
            print(f" - {f}")

        context["ti"].xcom_push(key="file_list", value=file_list) # 다음 Task를 위해 Xcom에 저장
        
    # -------------------------------
    # Task 2: CSV 파일 읽고 중복 제거 (product_id 기준)
    # -------------------------------
    def dedup_records(**context):
        ti = context["ti"]
        file_list = ti.xcom_pull(task_ids="list_olive_files", key="file_list")

        client = storage.Client()
        bucket = client.bucket("bronze-layer-example")
        
        # 여러 CSV를 저장할 DataFrame list
        all_dfs = []
        for file_name in file_list:
            # 텍스트 형식으로 다운로드 받고 pandas.read_csv()를 위해 io.StringIO를 사용해 메모리에서 읽음
            blob = bucket.blob(file_name)
            content = blob.download_as_text(encoding="utf-8")
            df = pd.read_csv(io.StringIO(content))

            # 쉼표(,) 제거 후 float형 반환
            df["price"] = df["price"].astype(str).str.replace(",", "").astype(float)
            df["scraped_at"] = pd.to_datetime(df["scraped_at"])
            
            # 같은 product_id가 여러 번 존재할 경우 가장 최근 데이터 하나만 남김
            df = df.sort_values("scraped_at", ascending=False).drop_duplicates(subset=["product_id"])
            all_dfs.append(df)
        
        # 모든 CSV 파일을 하나의 DataFrame으로 결합
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # BigQuery에서 scraped_at이 STRING으로 인식되도록 문자열로 변환
        combined_df["scraped_at"] = combined_df["scraped_at"].astype(str)
    
        ti.xcom_push(key="deduped_df", value=combined_df.to_json(orient="records"))

    # -------------------------------
    # Task 3: 정제된 데이터를 BigQuery 임시 테이블로 적재
    # -------------------------------
    def load_temp_bq(**context):
        ti = context["ti"]
        json_data = ti.xcom_pull(task_ids="dedup_records", key="deduped_df")
        records = json.loads(json_data)
        df = pd.DataFrame(records)

        client = bigquery.Client()
        table_id = "final-project-practice-465301.silver.temp_oliveyoung_products"
        
        # 테이블 자동 스키마 감지, 덮어쓰기 모드
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )
        
        # DataFrame을 BigQuery로 적재
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"[oliveyoung] Loaded {len(df)} records to {table_id}")

    # -------------------------------
    # Task 4: Silver 테이블에 MERGE (upsert)
    # -------------------------------
    def merge_to_silver(**context):
        client = bigquery.Client()
        
        # temp 테이블과 silver 테이블을 병합 (upsert 로직)
        sql = """
        MERGE `final-project-practice-465301.silver.oliveyoung_products` T
        USING `final-project-practice-465301.silver.temp_oliveyoung_products` S
        ON T.product_id = S.product_id
        WHEN MATCHED THEN
          UPDATE SET
            name = S.name,
            brand = S.brand,
            price = S.price,
            url = S.url,
            rating = S.rating,
            review_count = S.review_count,
            scraped_at = CAST(S.scraped_at AS TIMESTAMP)
        WHEN NOT MATCHED THEN
          INSERT (product_id, name, brand, price, url, rating, review_count, scraped_at)
          VALUES (S.product_id, S.name, S.brand, S.price, S.url, S.rating, S.review_count, CAST(S.scraped_at AS TIMESTAMP))
        """

        try:
            query_job = client.query(sql)
            query_job.result()
            print(f"[oliveyoung] Merge to silver complete.")
        except Exception as e:
            print(f"[oliveyoung] Merge failed: {e}")
            raise e

    # Task 정의
    t1 = PythonOperator(
        task_id="list_olive_files",
        python_callable=list_olive_files,
    )

    t2 = PythonOperator(
        task_id="dedup_records",
        python_callable=dedup_records,
    )

    t3 = PythonOperator(
        task_id="load_temp_bq",
        python_callable=load_temp_bq,
    )

    t4 = PythonOperator(
        task_id="merge_to_silver",
        python_callable=merge_to_silver,
    )

    # 실행 순서
    t1 >> t2 >> t3 >> t4
