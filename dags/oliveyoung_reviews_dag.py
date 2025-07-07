"""
올리브영 리뷰 수집 DAG
리뷰 크롤러 배포 및 수집된 상품 데이터를 기반으로 각 상품의 리뷰를 수집하여 GCS에 저장
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum
from google.cloud import storage

# 로깅 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    'owner': 'de6-team2',
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
BUCKET_NAME = f"{PROJECT_ID}-raw-data"
CLOUD_RUN_JOB_NAME = "oliveyoung-review-scraper"

def deploy_review_scraper(**context):
    """리뷰 크롤러 Cloud Run 서비스 배포"""
    import subprocess
    
    try:
        logger.info("📦 리뷰 크롤러 Cloud Run 서비스 배포 시작...")
        
        # 소스 코드 디렉토리 경로
        source_dir = "/opt/airflow/scripts/cloud_run/review_scraper"
        docker_repo = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/oliveyoung-scrapers"
        
        # 1. Docker 이미지 빌드
        logger.info("🔨 Docker 이미지 빌드 중...")
        build_cmd = [
            'gcloud', 'builds', 'submit',
            '--tag', f'{docker_repo}/review-scraper:latest',
            '--project', PROJECT_ID,
            '--timeout', '20m',
            source_dir
        ]
        
        result = subprocess.run(build_cmd, capture_output=True, text=True, check=True)
        logger.info("✅ Docker 이미지 빌드 완료")
        
        # 2. Cloud Run 서비스 배포
        logger.info("🚢 Cloud Run 서비스 배포 중...")
        deploy_cmd = [
            'gcloud', 'run', 'deploy', 'oliveyoung-review-scraper',
            '--image', f'{docker_repo}/review-scraper:latest',
            '--platform', 'managed',
            '--region', REGION,
            '--project', PROJECT_ID,
            '--memory', '4Gi',
            '--cpu', '2',
            '--timeout', '3600',
            '--max-instances', '10',
            '--set-env-vars', f'PROJECT_ID={PROJECT_ID},GCS_BUCKET={PROJECT_ID}-raw-data',
            '--service-account', f'dataproc-serverless-sa@{PROJECT_ID}.iam.gserviceaccount.com',
            '--no-allow-unauthenticated',
            '--quiet'
        ]
        
        result = subprocess.run(deploy_cmd, capture_output=True, text=True, check=True)
        logger.info("✅ 리뷰 크롤러 Cloud Run 서비스 배포 완료")
        
        # 3. 서비스 URL 확인
        url_cmd = [
            'gcloud', 'run', 'services', 'describe', 'oliveyoung-review-scraper',
            '--region', REGION,
            '--project', PROJECT_ID,
            '--format', 'value(status.url)'
        ]
        
        result = subprocess.run(url_cmd, capture_output=True, text=True, check=True)
        service_url = result.stdout.strip()
        logger.info(f"📊 서비스 URL: {service_url}")
        
        return {
            'status': 'success',
            'service_url': service_url,
            'deployed_at': datetime.now().isoformat()
        }
        
    except subprocess.CalledProcessError as e:
        error_msg = f"배포 실패: {e.stderr if e.stderr else str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"배포 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def get_product_data_from_gcs(**context) -> List[Dict]:
    """GCS에서 최신 상품 데이터 로드"""
    execution_date = context['ds']
    
    try:
        # GCS 클라이언트 초기화
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        
        # 최신 상품 데이터 파일들 찾기
        prefix = "oliveyoung/products/"
        blobs = list(client.list_blobs(bucket, prefix=prefix))
        
        if not blobs:
            raise Exception("No product data found in GCS")
        
        # 최신 파일들만 필터링 (오늘 또는 최근 데이터)
        recent_blobs = []
        for blob in blobs:
            if blob.name.endswith('.json'):
                # 파일 경로에서 날짜 정보 추출
                if execution_date.replace('-', '') in blob.name or \
                   datetime.now().strftime('%Y%m%d') in blob.name:
                    recent_blobs.append(blob)
        
        if not recent_blobs:
            # 최신 파일들 중 가장 최근 것들 선택
            recent_blobs = sorted(blobs, key=lambda x: x.time_created, reverse=True)[:10]
        
        logger.info(f"Found {len(recent_blobs)} recent product data files")
        
        # 모든 상품 데이터 수집
        all_products = []
        
        for blob in recent_blobs:
            try:
                json_data = blob.download_as_text()
                data = json.loads(json_data)
                
                products = data.get('products', [])
                for product in products:
                    if product.get('url') and product.get('product_id'):
                        all_products.append({
                            'product_id': product.get('product_id'),
                            'product_name': product.get('name', ''),
                            'product_url': product.get('url'),
                            'category_name': product.get('category_name', ''),
                            'brand': product.get('brand', ''),
                            'price': product.get('price', ''),
                            'rating': product.get('rating', ''),
                            'review_count': product.get('review_count', ''),
                        })
                
                logger.info(f"Loaded {len(products)} products from {blob.name}")
                
            except Exception as e:
                logger.error(f"Error loading data from {blob.name}: {str(e)}")
                continue
        
        # 중복 제거 (product_id 기준)
        unique_products = {}
        for product in all_products:
            product_id = product['product_id']
            if product_id and product_id not in unique_products:
                unique_products[product_id] = product
        
        final_products = list(unique_products.values())
        logger.info(f"Total unique products for review scraping: {len(final_products)}")
        
        # 상품 수가 너무 많으면 제한 (리소스 고려)
        max_products = 100  # 한 번에 처리할 최대 상품 수
        if len(final_products) > max_products:
            final_products = final_products[:max_products]
            logger.info(f"Limited to {max_products} products for this execution")
        
        return final_products
        
    except Exception as e:
        logger.error(f"Error loading product data from GCS: {str(e)}")
        raise

def create_review_scraping_batches(products: List[Dict], batch_size: int = 10) -> List[List[Dict]]:
    """상품들을 배치로 나누기"""
    batches = []
    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        batches.append(batch)
    
    logger.info(f"Created {len(batches)} batches with {batch_size} products each")
    return batches

def execute_review_scraping_batch(batch: List[Dict], batch_number: int, **context):
    """배치 단위로 리뷰 크롤링 실행"""
    import requests
    
    # Cloud Run 서비스 URL (실제 배포 후 수정 필요)
    cloud_run_url = f"https://{CLOUD_RUN_JOB_NAME}-{REGION}.run.app"
    
    batch_results = []
    successful_scrapes = 0
    failed_scrapes = 0
    
    logger.info(f"Starting batch {batch_number} with {len(batch)} products")
    
    for product in batch:
        try:
            # 리뷰 크롤링 요청 데이터
            scraping_data = {
                'product_id': product['product_id'],
                'product_url': product['product_url'],
                'product_name': product['product_name'],
                'category_name': product['category_name'],
                'max_reviews': 50,  # 상품당 최대 리뷰 수
            }
            
            logger.info(f"Scraping reviews for: {product['product_name']} (ID: {product['product_id']})")
            
            # Cloud Run 서비스 호출
            response = requests.post(
                f"{cloud_run_url}/scrape",
                json=scraping_data,
                timeout=1800,  # 30분 타임아웃
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                result['batch_number'] = batch_number
                batch_results.append(result)
                successful_scrapes += 1
                
                reviews_count = result.get('reviews_count', 0)
                logger.info(f"✅ {product['product_name']}: {reviews_count} reviews")
                
            else:
                error_msg = f"Failed to scrape reviews for {product['product_name']}: {response.status_code}"
                logger.error(error_msg)
                failed_scrapes += 1
                
                batch_results.append({
                    'status': 'error',
                    'product_id': product['product_id'],
                    'product_name': product['product_name'],
                    'error': error_msg,
                    'batch_number': batch_number
                })
                
        except Exception as e:
            error_msg = f"Exception scraping {product['product_name']}: {str(e)}"
            logger.error(error_msg)
            failed_scrapes += 1
            
            batch_results.append({
                'status': 'error',
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'error': error_msg,
                'batch_number': batch_number
            })
    
    batch_summary = {
        'batch_number': batch_number,
        'total_products': len(batch),
        'successful_scrapes': successful_scrapes,
        'failed_scrapes': failed_scrapes,
        'results': batch_results
    }
    
    logger.info(f"Batch {batch_number} completed: {successful_scrapes} success, {failed_scrapes} failed")
    return batch_summary

def validate_review_scraping_results(**context):
    """리뷰 크롤링 결과 검증 및 요약"""
    ti = context['ti']
    
    # 모든 배치 결과 수집
    total_products = 0
    total_reviews = 0
    successful_products = 0
    failed_products = 0
    batch_summaries = []
    
    # 배치 태스크들의 결과 수집
    for i in range(10):  # 최대 10개 배치 가정
        task_id = f"scrape_reviews_batch_{i}"
        try:
            batch_result = ti.xcom_pull(task_ids=task_id)
            if batch_result:
                batch_summaries.append(batch_result)
                total_products += batch_result.get('total_products', 0)
                successful_products += batch_result.get('successful_scrapes', 0)
                failed_products += batch_result.get('failed_scrapes', 0)
                
                # 각 배치의 리뷰 수 합계
                for result in batch_result.get('results', []):
                    if result.get('status') == 'completed':
                        total_reviews += result.get('reviews_count', 0)
                
        except Exception as e:
            logger.warning(f"Could not get result from {task_id}: {str(e)}")
            continue
    
    # 전체 요약
    summary = {
        'execution_date': context['ds'],
        'total_batches': len(batch_summaries),
        'total_products_processed': total_products,
        'successful_products': successful_products,
        'failed_products': failed_products,
        'total_reviews_scraped': total_reviews,
        'success_rate': (successful_products / total_products * 100) if total_products > 0 else 0,
        'batch_details': batch_summaries,
        'scraping_completed_at': datetime.now().isoformat()
    }
    
    logger.info("=== 올리브영 리뷰 크롤링 완료 ===")
    logger.info(f"총 상품: {summary['total_products_processed']}")
    logger.info(f"성공: {summary['successful_products']}")
    logger.info(f"실패: {summary['failed_products']}")
    logger.info(f"총 리뷰 수: {summary['total_reviews_scraped']}")
    logger.info(f"성공률: {summary['success_rate']:.1f}%")
    
    # 실패율이 높으면 경고
    if summary['success_rate'] < 70:
        logger.warning(f"성공률이 낮습니다: {summary['success_rate']:.1f}%")
    
    return summary

# DAG 생성
dag = DAG(
    'oliveyoung_reviews_collection',
    default_args=default_args,
    description='올리브영 리뷰 크롤러 배포 및 데이터 수집',
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    max_active_runs=1,
    tags=['oliveyoung', 'reviews', 'scraping', 'deployment', 'manual'],
)

# Cloud Run 서비스 배포 태스크
deploy_service = PythonOperator(
    task_id='deploy_review_scraper_service',
    python_callable=deploy_review_scraper,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# 상품 데이터 로드 태스크
load_product_data = PythonOperator(
    task_id='load_product_data_from_gcs',
    python_callable=get_product_data_from_gcs,
    dag=dag,
)

# 배치 생성 태스크 (동적으로 생성)
def create_review_batches(**context):
    """리뷰 크롤링 배치 생성"""
    ti = context['ti']
    products = ti.xcom_pull(task_ids='load_product_data_from_gcs')
    
    if not products:
        raise Exception("No products data found")
    
    # 배치 크기 설정 (리소스에 따라 조정)
    batch_size = 10
    batches = create_review_scraping_batches(products, batch_size)
    
    return batches

create_batches = PythonOperator(
    task_id='create_review_batches',
    python_callable=create_review_batches,
    dag=dag,
)

# 배치별 리뷰 크롤링 태스크들 (최대 10개 배치)
def create_batch_tasks():
    """배치별 태스크 동적 생성"""
    batch_tasks = []
    
    for i in range(10):  # 최대 10개 배치
        def create_batch_task(batch_idx):
            def execute_batch(**context):
                ti = context['ti']
                batches = ti.xcom_pull(task_ids='create_review_batches')
                
                if not batches or batch_idx >= len(batches):
                    logger.info(f"No batch {batch_idx} to process")
                    return None
                
                batch = batches[batch_idx]
                return execute_review_scraping_batch(batch, batch_idx, **context)
            
            return PythonOperator(
                task_id=f'scrape_reviews_batch_{batch_idx}',
                python_callable=execute_batch,
                dag=dag,
                # pool='review_scraping_pool',  # 동시 실행 제한 - Composer 환경에서 확인 필요
            )
        
        batch_task = create_batch_task(i)
        batch_tasks.append(batch_task)
    
    return batch_tasks

batch_tasks = create_batch_tasks()

# 결과 검증 태스크
validate_results = PythonOperator(
    task_id='validate_review_scraping_results',
    python_callable=validate_review_scraping_results,
    dag=dag,
)

# 완료 알림 태스크
completion_notification = BashOperator(
    task_id='completion_notification',
    bash_command='''
    echo "=== 올리브영 리뷰 크롤링 완료 ==="
    echo "실행 날짜: {{ ds }}"
    echo "완료 시각: $(date)"
    echo "수집된 데이터는 GCS에 저장되었습니다."
    ''',
    dag=dag,
)

# 태스크 의존성 설정
# 1. 먼저 Cloud Run 서비스 배포
# 2. 배포 완료 후 상품 데이터 로드
# 3. 배치 생성 및 리뷰 크롤링 병렬 실행
# 4. 결과 검증 및 완료 알림

deploy_service >> load_product_data >> create_batches >> batch_tasks >> validate_results >> completion_notification

# 배치 태스크들 병렬 실행 설정
for task in batch_tasks:
    create_batches >> task