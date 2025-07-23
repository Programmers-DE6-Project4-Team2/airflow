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
    dag_id="bronze_musinsa_reviews",
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
            autodetect=False,  # 자동 감지 끄기
            schema=[
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("category_code", "INTEGER"),
                bigquery.SchemaField("category_name", "STRING"),
                bigquery.SchemaField("no", "INTEGER"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("typeName", "STRING"),
                bigquery.SchemaField("subType", "STRING"),
                bigquery.SchemaField("content", "STRING"),
                bigquery.SchemaField("commentCount", "INTEGER"),
                bigquery.SchemaField("grade", "INTEGER"),
                bigquery.SchemaField("goods_goodsNo", "INTEGER"),
                bigquery.SchemaField("goods_goodsSubNo", "INTEGER"),
                bigquery.SchemaField("goods_goodsName", "STRING"),
                bigquery.SchemaField("goods_goodsImageFile", "STRING"),
                bigquery.SchemaField("goods_goodsImageExtension", "STRING"),
                bigquery.SchemaField("goods_goodsOptionKindCode", "STRING"),
                bigquery.SchemaField("goods_brandName", "STRING"),
                bigquery.SchemaField("goods_brandEnglishName", "STRING"),
                bigquery.SchemaField("goods_brand", "STRING"),
                bigquery.SchemaField("goods_brandBestYn", "BOOLEAN"),
                bigquery.SchemaField("goods_brandConcatenation", "STRING"),
                bigquery.SchemaField("goods_goodsCreateDate", "TIMESTAMP"),
                bigquery.SchemaField("goods_goodsImageIdx", "INTEGER"),
                bigquery.SchemaField("goods_saleStatCode", "STRING"),
                bigquery.SchemaField("goods_saleStatLabel", "STRING"),
                bigquery.SchemaField("goods_goodsSex", "INTEGER"),
                bigquery.SchemaField("goods_goodsSexClassification", "STRING"),
                bigquery.SchemaField("goods_showSoldOut", "BOOLEAN"),
                bigquery.SchemaField("userImageFile", "STRING"),
                bigquery.SchemaField("goodsOption", "STRING"),
                bigquery.SchemaField("commentReplyCount", "INTEGER"),
                bigquery.SchemaField("userStaffYn", "BOOLEAN"),
                bigquery.SchemaField("images", "STRING"),
                bigquery.SchemaField("likeCount", "INTEGER"),
                bigquery.SchemaField("userReactionType", "STRING"),
                bigquery.SchemaField("createDate", "TIMESTAMP"),
                bigquery.SchemaField("goodsThumbnailImageUrl", "STRING"),
                bigquery.SchemaField("userId", "STRING"),
                bigquery.SchemaField("encryptedUserId", "STRING"),
                bigquery.SchemaField("userProfileInfo_userNickName", "STRING"),
                bigquery.SchemaField("userProfileInfo_userLevel", "INTEGER"),
                bigquery.SchemaField("userProfileInfo_userOutYn", "BOOLEAN"),
                bigquery.SchemaField("userProfileInfo_userStaffYn", "BOOLEAN"),
                bigquery.SchemaField("userProfileInfo_reviewSex", "STRING"),
                bigquery.SchemaField("userProfileInfo_userWeight", "INTEGER"),
                bigquery.SchemaField("userProfileInfo_userHeight", "INTEGER"),
                bigquery.SchemaField("userProfileInfo_userSkinInfo", "STRING"),
                bigquery.SchemaField("userProfileInfo_skinType", "STRING"),
                bigquery.SchemaField("userProfileInfo_skinTone", "STRING"),
                bigquery.SchemaField("userProfileInfo_skinWorry", "STRING"),
                bigquery.SchemaField("orderOptionNo", "INTEGER"),
                bigquery.SchemaField("channelSource", "STRING"),
                bigquery.SchemaField("channelSourceName", "STRING"),
                bigquery.SchemaField("channelActivityId", "STRING"), 
                bigquery.SchemaField("relatedNo", "INTEGER"),
                bigquery.SchemaField("isFirstReview", "INTEGER"),
                bigquery.SchemaField("reviewProfileTypeEnum", "STRING"),
                bigquery.SchemaField("specialtyCodes", "STRING"),
                bigquery.SchemaField("reviewerWeeklyRanking", "STRING"),
                bigquery.SchemaField("reviewerMonthlyRanking", "STRING"),
                bigquery.SchemaField("showUserProfile", "BOOLEAN"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP"),
            ],
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

