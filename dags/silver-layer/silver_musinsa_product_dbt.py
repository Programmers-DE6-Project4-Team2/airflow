from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
}

with DAG(
    dag_id="silver_musinsa_product_dbt",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # 수동 실행 또는 upstream에 따라 설정
    catchup=False,
    description="Run dbt model for Musinsa product silver layer",
    tags=["silver", "musinsa", "dbt"],
) as dag:

    run_dbt_model = BashOperator(
        task_id="run_dbt_musinsa_model",
        bash_command="""
        source ~/dbt-venv/bin/activate
        cd /home/elhanan/airflow/dbt/beauty_elt
        dbt run --select musinsa_products
        """,
    )