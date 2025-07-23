from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "myksphone2001@gmail.com",
    "retries": 1,
}

with DAG(
    dag_id="gold_reviews_dbt_run",
    start_date=days_ago(1),
    schedule_interval=None,  # 수동 실행 또는 상위 DAG에서 Trigger
    catchup=False,
    tags=["gold", "dbt", "review"]
) as dag:

    run_dbt_gold_products = BashOperator(
    task_id="run_dbt_gold_reviews_model",
    bash_command="""
    dbt run \
        --project-dir /opt/airflow/dbt/beauty_elt \
        --profiles-dir /opt/airflow/.dbt \
        --select fact_reviews \
        --target gold
    """,
)
