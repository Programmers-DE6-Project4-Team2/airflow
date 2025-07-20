"""
Daily Time Display DAG
매일 UTC 0시에 현재 UTC와 KST 시간을 출력하는 DAG
"""

import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG 기본 설정
default_args = {
    'owner': 'dawit0905@gmail.com',
    'depends_on_past': False,
    'start_date': pendulum.now('UTC').subtract(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def display_current_times(**context):
    """현재 UTC와 KST 시간을 출력하는 함수"""
    
    # 현재 UTC 시간
    utc_now = pendulum.now('UTC')
    
    # KST 시간 (UTC+9)
    kst_now = pendulum.now('Asia/Seoul')
    
    print("=" * 50)
    print("🕐 Daily Time Display")
    print("=" * 50)
    print(f"📅 Execution Date: {context['ds']}")
    print(f"⏰ Current UTC Time: {utc_now.format('YYYY-MM-DD HH:mm:ss')} UTC")
    print(f"🇰🇷 Current KST Time: {kst_now.format('YYYY-MM-DD HH:mm:ss')} KST")
    print(f"📊 Time Difference: UTC+9 hours")
    print("=" * 50)
    
    return {
        'utc_time': utc_now.format('YYYY-MM-DD HH:mm:ss'),
        'kst_time': kst_now.format('YYYY-MM-DD HH:mm:ss'),
        'execution_date': context['ds']
    }

# DAG 생성
dag = DAG(
    'daily_time_display',
    default_args=default_args,
    description='매일 UTC 0시에 UTC와 KST 시간을 출력',
    schedule_interval='0 0 * * *',  # 매일 UTC 0시
    catchup=False,
    max_active_runs=1,
    tags=['daily', 'time', 'monitoring'],
)

# 시간 출력 태스크
display_time_task = PythonOperator(
    task_id='display_current_times',
    python_callable=display_current_times,
    dag=dag,
)

# 시간 정보를 bash로도 출력
bash_time_display = BashOperator(
    task_id='bash_time_display',
    bash_command='''
    echo "=========================================="
    echo "🐚 Bash Time Display"
    echo "=========================================="
    echo "⏰ UTC Time: $(date -u '+%Y-%m-%d %H:%M:%S') UTC"
    echo "🇰🇷 KST Time: $(TZ='Asia/Seoul' date '+%Y-%m-%d %H:%M:%S') KST"
    echo "📅 Execution Date: {{ ds }}"
    echo "🚀 DAG Run ID: {{ dag_run.run_id }}"
    echo "=========================================="
    ''',
    dag=dag,
)

# 완료 알림 태스크
completion_task = BashOperator(
    task_id='completion_notification',
    bash_command='''
    echo "✅ Daily time display completed successfully!"
    echo "📊 Next execution: Tomorrow at 00:00 UTC"
    echo "🔄 This DAG runs daily at midnight UTC"
    ''',
    dag=dag,
)

# 태스크 의존성 설정
display_time_task >> bash_time_display >> completion_task