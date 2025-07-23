from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "h2k997183@gmail.com",
    "retries": 1,
}

with DAG(
    dag_id="silver_oliveyoung_review_dbt",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    description="Run dbt model for OliveYoung review silver layer",
    tags=["silver", "oliveyoung", "dbt"],
) as dag:

    run_dbt_model = BashOperator(
        task_id="run_dbt_oliveyoung_review_model",
        bash_command="""
        dbt run \
            --project-dir /opt/airflow/dbt/beauty_elt \
            --profiles-dir /opt/airflow/.dbt \
            --select oliveyoung_reviews
        """,
    )
