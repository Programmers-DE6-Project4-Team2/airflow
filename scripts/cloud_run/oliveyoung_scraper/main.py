"""
올리브영 크롤러 Cloud Run 메인 애플리케이션
"""

import os
import logging
from flask import Flask, request, jsonify
from datetime import datetime
import traceback

from scraper import OliveYoungScraper

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask 앱 생성
app = Flask(__name__)

# 환경 변수 설정
PROJECT_ID = os.getenv('PROJECT_ID', 'de6-2ez')
GCS_BUCKET = os.getenv('GCS_BUCKET', 'de6-2ez-raw-data')

@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크 엔드포인트"""
    return jsonify({
        'status': 'healthy',
        'service': 'oliveyoung-scraper',
        'version': '1.0.0',
        'timestamp': datetime.now().isoformat(),
        'project_id': PROJECT_ID,
        'gcs_bucket': GCS_BUCKET
    })

@app.route('/info', methods=['GET'])
def service_info():
    """서비스 정보 엔드포인트"""
    return jsonify({
        'service_name': 'oliveyoung-scraper',
        'description': '올리브영 상품 데이터 크롤링 서비스',
        'version': '1.0.0',
        'supported_endpoints': [
            'GET /health - 헬스 체크',
            'GET /info - 서비스 정보',
            'POST /scrape - 카테고리 크롤링'
        ],
        'project_id': PROJECT_ID,
        'gcs_bucket': GCS_BUCKET,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/scrape', methods=['POST'])
def scrape_category():
    """카테고리 크롤링 엔드포인트"""
    try:
        # 요청 데이터 검증
        if not request.is_json:
            return jsonify({
                'status': 'error',
                'error_message': 'Content-Type must be application/json'
            }), 400
        
        data = request.get_json()
        
        # 필수 파라미터 검증
        required_params = ['category_name', 'category_url']
        for param in required_params:
            if param not in data:
                return jsonify({
                    'status': 'error',
                    'error_message': f'Missing required parameter: {param}'
                }), 400
        
        category_name = data['category_name']
        category_url = data['category_url']
        max_pages = data.get('max_pages', 5)
        
        logger.info(f"카테고리 크롤링 시작: {category_name}")
        logger.info(f"URL: {category_url}")
        logger.info(f"최대 페이지: {max_pages}")
        
        # 크롤러 인스턴스 생성 및 실행
        scraper = OliveYoungScraper(
            gcs_bucket=GCS_BUCKET,
            project_id=PROJECT_ID
        )
        
        # 크롤링 실행
        result = scraper.scrape_category(
            category_name=category_name,
            category_url=category_url,
            max_pages=max_pages
        )
        
        # 결과 반환
        if result['status'] == 'success':
            logger.info(f"크롤링 완료: {category_name}")
            logger.info(f"수집된 상품 수: {result['products_count']}")
            logger.info(f"GCS 저장 경로: {result['gcs_path']}")
            
            return jsonify(result), 200
        else:
            logger.error(f"크롤링 실패: {category_name}")
            logger.error(f"오류: {result.get('error_message', '')}")
            
            return jsonify(result), 500
            
    except Exception as e:
        error_msg = f"크롤링 처리 중 예외 발생: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        return jsonify({
            'status': 'error',
            'error_message': error_msg,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    """404 에러 핸들러"""
    return jsonify({
        'status': 'error',
        'error_message': 'Endpoint not found',
        'available_endpoints': ['/health', '/info', '/scrape']
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """500 에러 핸들러"""
    return jsonify({
        'status': 'error',
        'error_message': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500

if __name__ == '__main__':
    # 개발 환경에서는 debug=True, 프로덕션에서는 Gunicorn 사용
    port = int(os.environ.get('PORT', 8080))
    
    logger.info("=== 올리브영 크롤러 서비스 시작 ===")
    logger.info(f"포트: {port}")
    logger.info(f"프로젝트 ID: {PROJECT_ID}")
    logger.info(f"GCS 버킷: {GCS_BUCKET}")
    
    app.run(host='0.0.0.0', port=port, debug=False)