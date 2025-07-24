from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "myksphone2001@gmail.com",
    "retries": 1,
}

with DAG(
    dag_id="gold_products_dbt_run",
    start_date=days_ago(1),
    schedule_interval="0 15 * * *", # 매일 오후 3시 UTC → 한국 시간 기준 자정
    catchup=False,
    tags=["gold", "dbt", "product"]
) as dag:

    run_dbt_gold_products = BashOperator(
    task_id="run_dbt_gold_products_model",
    bash_command="""
    dbt run \
        --project-dir /opt/airflow/dbt/beauty_elt \
        --profiles-dir /opt/airflow/.dbt \
        --select fact_products \
        --target gold
    """,
)
