"""
올리브영 상품 수집 DAG
카테고리별로 상품 데이터를 수집하여 GCS에 저장
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# Google Cloud providers - not needed for manual Cloud Run deployment
from airflow.models import Variable
from airflow.utils.dates import days_ago

# 로깅 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    'owner': 'dawit0905@gmail.com',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# 카테고리 딕셔너리 (categories.py에서 가져온 데이터)
OLIVEYOUNG_CATEGORIES = {
    "스킨케어_스킨/토너": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=1000001000100130001&fltDispCatNo=&prdSort=01&pageIdx=1&rowsPerPage=24&searchTypeSort=btn_thumb&plusButtonFlag=N&isLoginCnt=1&aShowCnt=0&bShowCnt=0&cShowCnt=0&trackingCd=Cat1000001000100130001_Small&amplitudePageGubun=&t_page=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EA%B4%80&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EC%83%81%EC%84%B8_%EC%86%8C%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&midCategory=%EC%8A%A4%ED%82%A8%2F%ED%86%A0%EB%84%88&smallCategory=%EC%A0%84%EC%B2%B4&checkBrnds=&lastChkBrnd=&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4&t_2nd_category_type=%EC%A4%91_%EC%8A%A4%ED%82%A8%2F%ED%86%A0%EB%84%88&t_3rd_category_type=%EC%86%8C_%EC%A0%84%EC%B2%B4",
    "스킨케어_에센스/세럼/앰플": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010014&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&t_page=%EB%A1%9C%EC%BC%80%EC%9D%B4%EC%85%98_%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EA%B4%80&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%ED%83%AD_%EC%A4%91%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4&t_2nd_category_type=%EC%A4%91_%EC%97%90%EC%84%BC%EC%8A%A4/%EC%84%B8%EB%9F%BC/%EC%95%B0%ED%94%8C",
    "스킨케어_크림": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010015&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&t_page=%EB%A1%9C%EC%BC%80%EC%9D%B4%EC%85%98_%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EA%B4%80&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%ED%83%AD_%EC%A4%91%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4&t_2nd_category_type=%EC%A4%91_%ED%81%AC%EB%A6%BC",
    "스킨케어_로션": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010016&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&t_page=%EB%A1%9C%EC%BC%80%EC%9D%B4%EC%85%98_%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EA%B4%80&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%ED%83%AD_%EC%A4%91%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4&t_2nd_category_type=%EC%A4%91_%EB%A1%9C%EC%85%98",
    "스킨케어_미스트/오일": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010010&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&t_page=%EB%A1%9C%EC%BC%80%EC%9D%B4%EC%85%98_%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EA%B4%80&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%ED%83%AD_%EC%A4%91%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4&t_2nd_category_type=%EC%A4%91_%EB%AF%B8%EC%8A%A4%ED%8A%B8/%EC%98%A4%EC%9D%BC",
    "스킨케어_스킨케어세트": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010017&isLoginCnt=0&aShowCnt=0&bShowCnt=0&cShowCnt=0&t_page=%EB%A1%9C%EC%BC%80%EC%9D%B4%EC%85%98_%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%EA%B4%80&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%ED%83%AD_%EC%A4%91%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4&t_2nd_category_type=%EC%A4%91_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4%EC%84%B8%ED%8A%B8",
}

# GCP 설정
PROJECT_ID = "de6-2ez"
REGION = "asia-northeast3"
CLOUD_RUN_JOB_NAME = "oliveyoung-product-scraper"

def create_safe_task_id(category_name: str) -> str:
    """카테고리 이름을 Airflow task_id에 사용 가능한 형태로 변환"""
    # 카테고리 이름을 영문으로 매핑
    category_mapping = {
        "스킨케어_스킨/토너": "skincare_skin_toner",
        "스킨케어_에센스/세럼/앰플": "skincare_essence_serum_ampoule", 
        "스킨케어_크림": "skincare_cream",
        "스킨케어_로션": "skincare_lotion",
        "스킨케어_미스트/오일": "skincare_mist_oil",
        "스킨케어_스킨케어세트": "skincare_sets"
    }
    
    return category_mapping.get(category_name, category_name.replace('/', '_').replace(' ', '_'))

def create_scraping_tasks(**context) -> List[Dict]:
    """각 카테고리별 크롤링 작업 생성"""
    tasks = []
    execution_date = context['ds']
    
    for category_name, category_url in OLIVEYOUNG_CATEGORIES.items():
        task_info = {
            'category_name': category_name,
            'category_url': category_url,
            'execution_date': execution_date,
            'max_pages': 5,  # 카테고리별 최대 페이지 수
        }
        tasks.append(task_info)
    
    logger.info(f"Created {len(tasks)} scraping tasks")
    return tasks

def execute_category_scraping(category_name: str, category_url: str, max_pages: int = 5, **context):
    """개별 카테고리 크롤링 실행"""
    from airflow.providers.http.operators.http import SimpleHttpOperator
    import requests
    
    # Cloud Run 서비스 URL (실제 배포 후 수정 필요)
    cloud_run_url = f"https://{CLOUD_RUN_JOB_NAME}-{REGION}.run.app"
    
    # 크롤링 요청 데이터
    scraping_data = {
        'category_name': category_name,
        'category_url': category_url,
        'max_pages': max_pages
    }
    
    try:
        logger.info(f"Starting scraping for category: {category_name}")
        
        # Cloud Run 서비스 호출
        response = requests.post(
            f"{cloud_run_url}/scrape",
            json=scraping_data,
            timeout=3600,  # 1시간 타임아웃
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Successfully scraped {category_name}: {result}")
            return result
        else:
            error_msg = f"Failed to scrape {category_name}: {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        logger.error(f"Error scraping category {category_name}: {str(e)}")
        raise

def deploy_product_scraper(**context):
    """상품 크롤러 Cloud Run 서비스 배포"""
    import subprocess
    
    try:
        logger.info("📦 상품 크롤러 Cloud Run 서비스 배포 시작...")
        
        # 소스 코드 디렉토리 경로
        source_dir = "/opt/airflow/scripts/cloud_run/product_scraper"
        docker_repo = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/oliveyoung-scrapers"
        
        # 1. Docker 이미지 빌드
        logger.info("🔨 Docker 이미지 빌드 중...")
        build_cmd = [
            'gcloud', 'builds', 'submit',
            '--tag', f'{docker_repo}/product-scraper:latest',
            '--project', PROJECT_ID,
            '--timeout', '20m',
            source_dir
        ]
        
        result = subprocess.run(build_cmd, capture_output=True, text=True, check=True)
        logger.info("✅ Docker 이미지 빌드 완료")
        
        # 2. Cloud Run 서비스 배포
        logger.info("🚢 Cloud Run 서비스 배포 중...")
        deploy_cmd = [
            'gcloud', 'run', 'deploy', 'oliveyoung-product-scraper',
            '--image', f'{docker_repo}/product-scraper:latest',
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
        logger.info("✅ 상품 크롤러 Cloud Run 서비스 배포 완료")
        
        # 3. 서비스 URL 확인
        url_cmd = [
            'gcloud', 'run', 'services', 'describe', 'oliveyoung-product-scraper',
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
            if result and result.get('status') == 'completed':
                products_count = result.get('products_count', 0)
                total_products += products_count
                successful_categories.append({
                    'category': category_name,
                    'products_count': products_count,
                    'gcs_path': result.get('gcs_path')
                })
                logger.info(f"✅ {category_name}: {products_count} products")
            else:
                failed_categories.append(category_name)
                logger.error(f"❌ {category_name}: Failed")
        except Exception as e:
            failed_categories.append(category_name)
            logger.error(f"❌ {category_name}: Exception - {str(e)}")
    
    # 결과 요약
    summary = {
        'execution_date': context['ds'],
        'total_categories': len(OLIVEYOUNG_CATEGORIES),
        'successful_categories': len(successful_categories),
        'failed_categories': len(failed_categories),
        'total_products_scraped': total_products,
        'successful_category_details': successful_categories,
        'failed_category_list': failed_categories,
        'scraping_completed_at': datetime.now().isoformat()
    }
    
    logger.info("=== 올리브영 상품 크롤링 완료 ===")
    logger.info(f"총 카테고리: {summary['total_categories']}")
    logger.info(f"성공: {summary['successful_categories']}")
    logger.info(f"실패: {summary['failed_categories']}")
    logger.info(f"총 상품 수: {summary['total_products_scraped']}")
    
    # 실패한 카테고리가 있으면 경고
    if failed_categories:
        logger.warning(f"실패한 카테고리: {failed_categories}")
    
    return summary

# DAG 생성
dag = DAG(
    'oliveyoung_products_collection',
    default_args=default_args,
    description='올리브영 상품 크롤러 배포 및 데이터 수집',
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    max_active_runs=1,
    tags=['oliveyoung', 'products', 'scraping', 'deployment', 'manual'],
)

# Cloud Run 서비스 배포 태스크
deploy_service = PythonOperator(
    task_id='deploy_product_scraper_service',
    python_callable=deploy_product_scraper,
    execution_timeout=timedelta(minutes=30),
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

for category_name, category_url in OLIVEYOUNG_CATEGORIES.items():
    # 태스크 ID에 사용할 안전한 이름 생성
    safe_category_name = create_safe_task_id(category_name)
    
    scraping_task = PythonOperator(
        task_id=f'scrape_category_{safe_category_name}',
        python_callable=execute_category_scraping,
        op_kwargs={
            'category_name': category_name,
            'category_url': category_url,
            'max_pages': 5,
        },
        dag=dag,
        pool='default_pool',  # 기본 풀 사용
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
    echo "다음 단계: 리뷰 크롤링 DAG 실행 가능"
    ''',
    dag=dag,
)

# 태스크 의존성 설정
# 1. 먼저 Cloud Run 서비스 배포
# 2. 배포 완료 후 크롤링 작업 생성
# 3. 각 카테고리별 크롤링 병렬 실행
# 4. 결과 검증 및 완료 알림

deploy_service >> create_tasks >> scraping_tasks >> validate_results >> completion_notification

# 병렬 실행을 위한 설정
for task in scraping_tasks:
    create_tasks >> task
