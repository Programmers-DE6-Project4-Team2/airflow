from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("✅ DAG successfully deployed to GCS and recognized by Airflow.")

with DAG(
    dag_id="test_gcs_sync_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["test", "ci"],
) as dag:
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello,
    )
