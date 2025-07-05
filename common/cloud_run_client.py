"""
Cloud Run 클라이언트 공통 유틸리티
"""

import logging
import requests
import json
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class CloudRunClient:
    """Cloud Run 서비스 호출 클라이언트"""
    
    def __init__(self, service_url: str, timeout: int = 3600):
        """
        Cloud Run 클라이언트 초기화
        
        Args:
            service_url: Cloud Run 서비스 URL
            timeout: 요청 타임아웃 (초)
        """
        self.service_url = service_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        
        # 기본 헤더 설정
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Airflow-CloudRun-Client/1.0'
        })
        
        logger.info(f"Cloud Run 클라이언트 초기화: {self.service_url}")
    
    def scrape_category(self, category_name: str, category_url: str, 
                       max_pages: int = 5) -> Dict[str, Any]:
        """
        카테고리 크롤링 요청
        
        Args:
            category_name: 카테고리 이름
            category_url: 카테고리 URL
            max_pages: 최대 페이지 수
            
        Returns:
            Dict[str, Any]: 크롤링 결과
        """
        endpoint = f"{self.service_url}/scrape"
        
        payload = {
            'category_name': category_name,
            'category_url': category_url,
            'max_pages': max_pages,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            logger.info(f"카테고리 크롤링 시작: {category_name}")
            logger.info(f"요청 URL: {endpoint}")
            logger.info(f"최대 페이지 수: {max_pages}")
            
            start_time = time.time()
            
            response = self.session.post(
                endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            elapsed_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"크롤링 완료: {category_name} ({elapsed_time:.2f}초)")
                logger.info(f"수집된 상품 수: {result.get('products_count', 0)}")
                
                return {
                    'status': 'success',
                    'category_name': category_name,
                    'products_count': result.get('products_count', 0),
                    'gcs_path': result.get('gcs_path', ''),
                    'pages_scraped': result.get('pages_scraped', 0),
                    'elapsed_time': elapsed_time,
                    'message': result.get('message', ''),
                    'scraped_at': result.get('scraped_at', datetime.now().isoformat())
                }
            else:
                error_msg = f"크롤링 실패: {category_name} - HTTP {response.status_code}"
                logger.error(error_msg)
                logger.error(f"응답 내용: {response.text}")
                
                return {
                    'status': 'error',
                    'category_name': category_name,
                    'error_code': response.status_code,
                    'error_message': response.text,
                    'elapsed_time': elapsed_time
                }
                
        except requests.exceptions.Timeout:
            error_msg = f"크롤링 타임아웃: {category_name} (>{self.timeout}초)"
            logger.error(error_msg)
            return {
                'status': 'timeout',
                'category_name': category_name,
                'error_message': error_msg,
                'timeout': self.timeout
            }
            
        except requests.exceptions.ConnectionError:
            error_msg = f"연결 오류: {category_name} - Cloud Run 서비스 연결 실패"
            logger.error(error_msg)
            return {
                'status': 'connection_error',
                'category_name': category_name,
                'error_message': error_msg
            }
            
        except Exception as e:
            error_msg = f"예외 발생: {category_name} - {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'exception',
                'category_name': category_name,
                'error_message': error_msg
            }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Cloud Run 서비스 헬스 체크
        
        Returns:
            Dict[str, Any]: 헬스 체크 결과
        """
        endpoint = f"{self.service_url}/health"
        
        try:
            response = self.session.get(endpoint, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                logger.info("헬스 체크 성공")
                return {
                    'status': 'healthy',
                    'service_url': self.service_url,
                    'response_time': response.elapsed.total_seconds(),
                    'version': result.get('version', 'unknown'),
                    'timestamp': result.get('timestamp', datetime.now().isoformat())
                }
            else:
                logger.warning(f"헬스 체크 실패: HTTP {response.status_code}")
                return {
                    'status': 'unhealthy',
                    'service_url': self.service_url,
                    'error_code': response.status_code,
                    'error_message': response.text
                }
                
        except Exception as e:
            logger.error(f"헬스 체크 예외: {str(e)}")
            return {
                'status': 'error',
                'service_url': self.service_url,
                'error_message': str(e)
            }
    
    def get_service_info(self) -> Dict[str, Any]:
        """
        Cloud Run 서비스 정보 조회
        
        Returns:
            Dict[str, Any]: 서비스 정보
        """
        endpoint = f"{self.service_url}/info"
        
        try:
            response = self.session.get(endpoint, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                logger.info("서비스 정보 조회 성공")
                return {
                    'status': 'success',
                    'service_info': result,
                    'service_url': self.service_url
                }
            else:
                logger.warning(f"서비스 정보 조회 실패: HTTP {response.status_code}")
                return {
                    'status': 'error',
                    'error_code': response.status_code,
                    'error_message': response.text
                }
                
        except Exception as e:
            logger.error(f"서비스 정보 조회 예외: {str(e)}")
            return {
                'status': 'error',
                'error_message': str(e)
            }
    
    def close(self):
        """세션 종료"""
        self.session.close()
        logger.info("Cloud Run 클라이언트 세션 종료")


class CloudRunBatchClient:
    """Cloud Run 배치 처리 클라이언트"""
    
    def __init__(self, service_url: str, timeout: int = 3600, max_concurrent: int = 10):
        """
        배치 클라이언트 초기화
        
        Args:
            service_url: Cloud Run 서비스 URL
            timeout: 요청 타임아웃 (초)
            max_concurrent: 최대 동시 실행 수
        """
        self.client = CloudRunClient(service_url, timeout)
        self.max_concurrent = max_concurrent
        
        logger.info(f"Cloud Run 배치 클라이언트 초기화: 최대 동시 실행 {max_concurrent}")
    
    def scrape_categories_batch(self, categories: Dict[str, Dict]) -> Dict[str, Any]:
        """
        여러 카테고리 배치 크롤링
        
        Args:
            categories: 카테고리 딕셔너리 {name: {url, max_pages}}
            
        Returns:
            Dict[str, Any]: 배치 처리 결과
        """
        results = {}
        successful_count = 0
        failed_count = 0
        
        logger.info(f"배치 크롤링 시작: {len(categories)}개 카테고리")
        
        for category_name, category_info in categories.items():
            try:
                result = self.client.scrape_category(
                    category_name=category_name,
                    category_url=category_info['url'],
                    max_pages=category_info.get('max_pages', 5)
                )
                
                results[category_name] = result
                
                if result['status'] == 'success':
                    successful_count += 1
                else:
                    failed_count += 1
                    
                # 요청 간 딜레이 (서비스 과부하 방지)
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"카테고리 {category_name} 처리 중 예외: {str(e)}")
                results[category_name] = {
                    'status': 'exception',
                    'category_name': category_name,
                    'error_message': str(e)
                }
                failed_count += 1
        
        summary = {
            'total_categories': len(categories),
            'successful_count': successful_count,
            'failed_count': failed_count,
            'success_rate': (successful_count / len(categories)) * 100,
            'results': results,
            'completed_at': datetime.now().isoformat()
        }
        
        logger.info(f"배치 크롤링 완료: 성공 {successful_count}, 실패 {failed_count}")
        return summary
    
    def close(self):
        """클라이언트 종료"""
        self.client.close()


def create_cloud_run_client(service_url: str, timeout: int = 3600) -> CloudRunClient:
    """
    Cloud Run 클라이언트 생성 헬퍼 함수
    
    Args:
        service_url: Cloud Run 서비스 URL
        timeout: 요청 타임아웃 (초)
        
    Returns:
        CloudRunClient: 초기화된 클라이언트
    """
    return CloudRunClient(service_url, timeout)


def create_batch_client(service_url: str, timeout: int = 3600, 
                       max_concurrent: int = 10) -> CloudRunBatchClient:
    """
    Cloud Run 배치 클라이언트 생성 헬퍼 함수
    
    Args:
        service_url: Cloud Run 서비스 URL
        timeout: 요청 타임아웃 (초)
        max_concurrent: 최대 동시 실행 수
        
    Returns:
        CloudRunBatchClient: 초기화된 배치 클라이언트
    """
    return CloudRunBatchClient(service_url, timeout, max_concurrent)