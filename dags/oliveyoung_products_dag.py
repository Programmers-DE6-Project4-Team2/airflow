"""
올리브영 상품 수집 DAG
카테고리별로 상품 데이터를 수집하여 GCS에 저장
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum

# Common 모듈 경로 추가
sys.path.append('/opt/airflow/common')

# Common 모듈 import
from categories import OLIVEYOUNG_CATEGORIES, create_safe_task_id, get_gcs_path
from cloud_run_client import create_cloud_run_client
from gcs_uploader import create_gcs_uploader
from data_schemas import validate_oliveyoung_data

# 로깅 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    'owner': 'dawit0905@gmail.com',
    'depends_on_past': False,
    'start_date': pendulum.now('UTC').subtract(days=1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# GCP 설정
PROJECT_ID = "de6-2ez"
REGION = "asia-northeast3"
CLOUD_RUN_SERVICE_NAME = "oliveyoung-product-scraper"
GCS_BUCKET_NAME = f"{PROJECT_ID}"

# Cloud Run 서비스 URL (Terraform으로 배포된 서비스)
# 기존 서비스를 활용하거나 Terraform output에서 가져올 수 있음
CLOUD_RUN_SERVICE_URL = "https://oliveyoung-product-scraper-rlvf5tevza-du.a.run.app"

def create_scraping_tasks(**context) -> List[Dict]:
    """각 카테고리별 크롤링 작업 생성"""
    tasks = []
    execution_date = context['ds']
    
    for category_name, category_info in OLIVEYOUNG_CATEGORIES.items():
        task_info = {
            'category_name': category_name,
            'category_url': category_info['url'],
            'execution_date': execution_date,
            'max_pages': category_info.get('max_pages', 5),
        }
        tasks.append(task_info)
    
    logger.info(f"Created {len(tasks)} scraping tasks for {len(OLIVEYOUNG_CATEGORIES)} categories")
    return tasks

def execute_category_scraping(category_name: str, category_url: str, max_pages: int = 5, **context):
    """개별 카테고리 크롤링 실행"""
    
    try:
        logger.info(f"카테고리 크롤링 시작: {category_name}")
        logger.info(f"최대 페이지: {max_pages}")
        logger.info(f"Cloud Run 서비스 URL: {CLOUD_RUN_SERVICE_URL}")
        
        # Cloud Run 클라이언트 생성
        client = create_cloud_run_client(CLOUD_RUN_SERVICE_URL)
        
        # 크롤링 실행
        result = client.scrape_category(
            category_name=category_name,
            category_url=category_url,
            max_pages=max_pages
        )
        
        # 결과 확인
        if result['status'] == 'success':
            logger.info(f"크롤링 완료: {category_name}")
            logger.info(f"수집된 상품 수: {result.get('products_count', 0)}")
            logger.info(f"GCS 저장 경로: {result.get('gcs_path', '')}")
            return result
        else:
            error_msg = f"크롤링 실패: {category_name} - {result.get('error_message', '')}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        logger.error(f"카테고리 크롤링 오류 {category_name}: {str(e)}")
        raise
    finally:
        # 클라이언트 정리
        if 'client' in locals():
            client.close()

def check_cloud_run_service(**context):
    """Cloud Run 서비스 상태 확인"""
    try:
        logger.info(f"Cloud Run 서비스 헬스 체크: {CLOUD_RUN_SERVICE_URL}")
        
        # Cloud Run 클라이언트 생성
        client = create_cloud_run_client(CLOUD_RUN_SERVICE_URL)
        
        # 헬스 체크 실행
        health_result = client.health_check()
        
        if health_result['status'] == 'healthy':
            logger.info("✅ Cloud Run 서비스가 정상 작동 중입니다")
            return {
                'status': 'healthy',
                'service_url': CLOUD_RUN_SERVICE_URL,
                'version': health_result.get('version', 'unknown'),
                'checked_at': datetime.now().isoformat()
            }
        else:
            error_msg = f"❌ Cloud Run 서비스 상태 불량: {health_result.get('error_message', '')}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        logger.error(f"헬스 체크 실패: {str(e)}")
        raise
    finally:
        # 클라이언트 정리
        if 'client' in locals():
            client.close()

def validate_scraping_results(**context):
    """크롤링 결과 검증 및 요약"""
    ti = context['ti']
    
    # 모든 카테고리 작업 결과 수집
    total_products = 0
    successful_categories = []
    failed_categories = []
    
    for category_name in OLIVEYOUNG_CATEGORIES.keys():
        task_id = f"scrape_category_{create_safe_task_id(category_name)}"
        try:
            result = ti.xcom_pull(task_ids=task_id)
            if result and result.get('status') == 'success':
                products_count = result.get('products_count', 0)
                total_products += products_count
                successful_categories.append({
                    'category': category_name,
                    'products_count': products_count,
                    'gcs_path': result.get('gcs_path', ''),
                    'pages_scraped': result.get('pages_scraped', 0),
                    'elapsed_time': result.get('elapsed_time', 0)
                })
                logger.info(f"✅ {category_name}: {products_count}개 상품, {result.get('pages_scraped', 0)}페이지")
            else:
                failed_categories.append({
                    'category': category_name,
                    'error_message': result.get('error_message', 'Unknown error') if result else 'No result returned'
                })
                logger.error(f"❌ {category_name}: 실패")
        except Exception as e:
            failed_categories.append({
                'category': category_name,
                'error_message': str(e)
            })
            logger.error(f"❌ {category_name}: 예외 발생 - {str(e)}")
    
    # 결과 요약
    summary = {
        'execution_date': context['ds'],
        'total_categories': len(OLIVEYOUNG_CATEGORIES),
        'successful_categories': len(successful_categories),
        'failed_categories': len(failed_categories),
        'total_products_scraped': total_products,
        'success_rate': (len(successful_categories) / len(OLIVEYOUNG_CATEGORIES)) * 100,
        'successful_category_details': successful_categories,
        'failed_category_details': failed_categories,
        'scraping_completed_at': datetime.now().isoformat()
    }
    
    logger.info("=== 올리브영 상품 크롤링 완료 ===")
    logger.info(f"총 카테고리: {summary['total_categories']}개")
    logger.info(f"성공: {summary['successful_categories']}개")
    logger.info(f"실패: {summary['failed_categories']}개")
    logger.info(f"성공률: {summary['success_rate']:.1f}%")
    logger.info(f"총 상품 수: {summary['total_products_scraped']}개")
    
    # 실패한 카테고리가 있으면 경고
    if failed_categories:
        failed_names = [item['category'] for item in failed_categories]
        logger.warning(f"실패한 카테고리: {failed_names}")
    
    return summary

# DAG 생성
dag = DAG(
    'oliveyoung_products_collection',
    default_args=default_args,
    description='올리브영 상품 데이터 수집',
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    max_active_runs=1,
    tags=['oliveyoung', 'products', 'scraping', 'manual'],
)

# Cloud Run 서비스 헬스 체크 태스크
health_check_service = PythonOperator(
    task_id='check_cloud_run_service',
    python_callable=check_cloud_run_service,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

# 작업 생성 태스크
create_tasks = PythonOperator(
    task_id='create_scraping_tasks',
    python_callable=create_scraping_tasks,
    dag=dag,
)

# 각 카테고리별 크롤링 태스크 동적 생성
scraping_tasks = []

for category_name, category_info in OLIVEYOUNG_CATEGORIES.items():
    # 태스크 ID에 사용할 안전한 이름 생성
    safe_category_name = create_safe_task_id(category_name)
    
    scraping_task = PythonOperator(
        task_id=f'scrape_category_{safe_category_name}',
        python_callable=execute_category_scraping,
        op_kwargs={
            'category_name': category_name,
            'category_url': category_info['url'],
            'max_pages': category_info.get('max_pages', 5),
        },
        dag=dag,
        # pool='default_pool',  # 기본 풀 사용 - Composer 환경에서 확인 필요
        execution_timeout=timedelta(minutes=60),  # 개별 태스크 타임아웃

    )
    
    scraping_tasks.append(scraping_task)

# 결과 검증 태스크
validate_results = PythonOperator(
    task_id='validate_scraping_results',
    python_callable=validate_scraping_results,
    dag=dag,
)

# 완료 알림 태스크
completion_notification = BashOperator(
    task_id='completion_notification',
    bash_command='''
    echo "=== 올리브영 상품 크롤링 완료 ==="
    echo "실행 날짜: {{ ds }}"
    echo "완료 시각: $(date)"
    echo "처리된 카테고리: {{ ti.xcom_pull(task_ids='validate_scraping_results')['total_categories'] }}개"
    echo "성공률: {{ ti.xcom_pull(task_ids='validate_scraping_results')['success_rate'] }}%"
    echo "총 상품 수: {{ ti.xcom_pull(task_ids='validate_scraping_results')['total_products_scraped'] }}개"
    echo "다음 단계: 리뷰 크롤링 DAG 실행 가능"
    ''',
    dag=dag,
)

# 태스크 의존성 설정
# 1. 먼저 Cloud Run 서비스 헬스 체크
# 2. 헬스 체크 완료 후 크롤링 작업 생성
# 3. 각 카테고리별 크롤링 병렬 실행
# 4. 결과 검증 및 완료 알림

health_check_service >> create_tasks >> scraping_tasks >> validate_results >> completion_notification

# 병렬 실행을 위한 설정
for task in scraping_tasks:
    create_tasks >> task
