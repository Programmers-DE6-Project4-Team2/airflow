"""
올리브영 크롤러 Cloud Run 구현
기존 PoC 크롤러를 Cloud Run 환경에 최적화한 버전
"""

import os
import logging
import time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from google.cloud import storage
import re
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)

class GCSUploader:
    """GCS 업로드 클래스"""
    
    def __init__(self, bucket_name: str, project_id: str):
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
    
    def upload_dataframe_as_csv(self, df: pd.DataFrame, gcs_path: str) -> bool:
        """DataFrame을 CSV로 GCS에 업로드"""
        try:
            csv_string = df.to_csv(index=False, encoding='utf-8-sig')
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(csv_string, content_type='text/csv')
            logger.info(f"CSV 업로드 완료: gs://{self.bucket_name}/{gcs_path}")
            return True
        except Exception as e:
            logger.error(f"CSV 업로드 실패: {str(e)}")
            return False

class OliveYoungScraper:
    """올리브영 크롤러 메인 클래스"""
    
    def __init__(self, gcs_bucket: str, project_id: str):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.driver = None
        self.gcs_uploader = GCSUploader(gcs_bucket, project_id)
        self.ua = UserAgent()
        
        # 상품 선택자 정의
        self.product_selectors = [
            'div.prd_info',
            'div.item',
            'li.item',
            'div.product-item',
            'div.prd-item'
        ]
        
        # 페이지네이션 선택자
        self.pagination_selectors = [
            'div.pageing a.next',
            'a.page-next',
            'div.pagination a.next'
        ]
    
    def setup_driver(self):
        """Chrome WebDriver 설정 (Cloud Run 최적화)"""
        try:
            options = webdriver.ChromeOptions()
            
            # Cloud Run에서 필수 옵션
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--window-size=1920,1080')
            
            # 메모리 최적화
            options.add_argument('--memory-pressure-off')
            options.add_argument('--max_old_space_size=4096')
            
            # 이미지 및 CSS 비활성화 (성능 향상)
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.stylesheets": 2
            }
            options.add_experimental_option("prefs", prefs)
            
            # 안티 디텍션
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # User Agent 설정
            user_agent = self.ua.random
            options.add_argument(f'--user-agent={user_agent}')
            
            # Chrome 바이너리 경로 (Cloud Run 컨테이너에서)
            options.binary_location = '/usr/bin/google-chrome'
            
            # ChromeDriver 서비스 설정
            service = Service('/usr/local/bin/chromedriver')
            
            # WebDriver 생성
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.set_page_load_timeout(30)
            
            # WebDriver 숨기기 스크립트 실행
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome WebDriver 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"WebDriver 초기화 실패: {str(e)}")
            return False
    
    def get_total_pages(self, category_url: str, max_pages: int = 5) -> int:
        """카테고리의 총 페이지 수 계산"""
        try:
            # 첫 페이지 로드
            self.driver.get(category_url)
            time.sleep(3)
            
            # 총 상품 수 추출 시도
            total_selectors = [
                'span.cate_prd_cnt',
                '.total-count',
                '.result-count',
                'span[class*="total"]',
                'span[class*="count"]'
            ]
            
            total_products = 0
            for selector in total_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    text = element.text
                    numbers = re.findall(r'\d+', text.replace(',', ''))
                    if numbers:
                        total_products = int(numbers[0])
                        break
                except:
                    continue
            
            if total_products > 0:
                # 페이지당 24개 상품으로 계산 (올리브영 기본값)
                calculated_pages = (total_products + 23) // 24
                actual_pages = min(calculated_pages, max_pages)
                logger.info(f"총 상품: {total_products}개, 계산된 페이지: {calculated_pages}, 크롤링 페이지: {actual_pages}")
                return actual_pages
            else:
                logger.warning("총 상품 수를 찾을 수 없음, 최대 페이지 수 사용")
                return max_pages
                
        except Exception as e:
            logger.error(f"페이지 수 계산 오류: {str(e)}")
            return max_pages
    
    def extract_products_from_page(self, page_num: int) -> List[Dict[str, Any]]:
        """현재 페이지에서 상품 정보 추출"""
        products = []
        
        try:
            # 페이지 로딩 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)
            
            # 상품 컨테이너 찾기
            product_elements = []
            for selector in self.product_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        product_elements = elements
                        logger.info(f"페이지 {page_num}: {selector} 선택자로 {len(elements)}개 상품 발견")
                        break
                except:
                    continue
            
            if not product_elements:
                logger.warning(f"페이지 {page_num}: 상품을 찾을 수 없음")
                return products
            
            # 각 상품 정보 추출
            for idx, element in enumerate(product_elements):
                try:
                    product_data = self.parse_product_element(element, page_num, idx)
                    if product_data:
                        products.append(product_data)
                except Exception as e:
                    logger.warning(f"페이지 {page_num}, 상품 {idx} 파싱 오류: {str(e)}")
                    continue
            
            logger.info(f"페이지 {page_num}: {len(products)}개 상품 추출 완료")
            
        except Exception as e:
            logger.error(f"페이지 {page_num} 상품 추출 오류: {str(e)}")
        
        return products
    
    def parse_product_element(self, element, page_num: int, idx: int) -> Optional[Dict[str, Any]]:
        """개별 상품 요소에서 정보 추출"""
        try:
            product_data = {
                'product_id': '',
                'name': '',
                'brand': '',
                'price': '',
                'original_price': '',
                'discount_rate': '',
                'rating': '',
                'review_count': '',
                'url': '',
                'image_url': '',
                'category': '',
                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'page_number': page_num
            }
            
            # 상품 URL 추출
            url_selectors = ['a[href*="/store/goods/"]', 'a[href*="product"]', 'a']
            for selector in url_selectors:
                try:
                    link_element = element.find_element(By.CSS_SELECTOR, selector)
                    href = link_element.get_attribute('href')
                    if href and 'goods' in href:
                        product_data['url'] = href
                        # URL에서 상품 ID 추출
                        match = re.search(r'goods.*?(\d+)', href)
                        if match:
                            product_data['product_id'] = match.group(1)
                        break
                except:
                    continue
            
            # 상품명 추출
            name_selectors = [
                'p.tx_name', 'div.tx_name', 'span.tx_name',
                '.prd_name', '.product-name', '.item-name'
            ]
            for selector in name_selectors:
                try:
                    name_element = element.find_element(By.CSS_SELECTOR, selector)
                    product_data['name'] = name_element.text.strip()
                    break
                except:
                    continue
            
            # 브랜드 추출
            brand_selectors = [
                'span.tx_brand', 'div.tx_brand', 'p.tx_brand',
                '.prd_brand', '.brand-name'
            ]
            for selector in brand_selectors:
                try:
                    brand_element = element.find_element(By.CSS_SELECTOR, selector)
                    product_data['brand'] = brand_element.text.strip()
                    break
                except:
                    continue
            
            # 가격 추출
            price_selectors = [
                'span.tx_cur strong', 'span.tx_sale strong',
                '.price_sale strong', '.current-price', '.sale-price'
            ]
            for selector in price_selectors:
                try:
                    price_element = element.find_element(By.CSS_SELECTOR, selector)
                    product_data['price'] = price_element.text.strip()
                    break
                except:
                    continue
            
            # 원가 추출
            original_price_selectors = [
                'span.tx_org', '.price_org', '.original-price'
            ]
            for selector in original_price_selectors:
                try:
                    orig_price_element = element.find_element(By.CSS_SELECTOR, selector)
                    product_data['original_price'] = orig_price_element.text.strip()
                    break
                except:
                    continue
            
            # 할인율 추출
            discount_selectors = [
                'span.tx_num', '.discount-rate', '.sale-percent'
            ]
            for selector in discount_selectors:
                try:
                    discount_element = element.find_element(By.CSS_SELECTOR, selector)
                    discount_text = discount_element.text.strip()
                    if '%' in discount_text:
                        product_data['discount_rate'] = discount_text
                        break
                except:
                    continue
            
            # 평점 추출
            rating_selectors = [
                'span.point', '.rating-score', '.star-rating'
            ]
            for selector in rating_selectors:
                try:
                    rating_element = element.find_element(By.CSS_SELECTOR, selector)
                    rating_text = rating_element.text.strip()
                    if rating_text:
                        product_data['rating'] = rating_text
                        break
                    
                    # 별점 이미지에서 width로 평점 계산
                    style = rating_element.get_attribute('style')
                    if style and 'width' in style:
                        width_match = re.search(r'width:\s*(\d+(?:\.\d+)?)%', style)
                        if width_match:
                            width_percent = float(width_match.group(1))
                            rating_score = round((width_percent / 100) * 5, 1)
                            product_data['rating'] = str(rating_score)
                            break
                except:
                    continue
            
            # 리뷰 수 추출
            review_selectors = [
                'span.tx_review', '.review-count', '.comment-count'
            ]
            for selector in review_selectors:
                try:
                    review_element = element.find_element(By.CSS_SELECTOR, selector)
                    review_text = review_element.text.strip()
                    # 숫자만 추출
                    numbers = re.findall(r'\d+', review_text.replace(',', ''))
                    if numbers:
                        product_data['review_count'] = numbers[0]
                        break
                except:
                    continue
            
            # 이미지 URL 추출
            img_selectors = ['img', '.product-image img', '.item-image img']
            for selector in img_selectors:
                try:
                    img_element = element.find_element(By.CSS_SELECTOR, selector)
                    img_src = img_element.get_attribute('src')
                    if img_src and ('oliveyoung' in img_src or 'product' in img_src):
                        product_data['image_url'] = img_src
                        break
                except:
                    continue
            
            # 필수 필드 검증
            if product_data['name'] and product_data['url']:
                return product_data
            else:
                logger.warning(f"페이지 {page_num}, 상품 {idx}: 필수 필드 누락")
                return None
                
        except Exception as e:
            logger.error(f"상품 파싱 오류: {str(e)}")
            return None
    
    def navigate_to_next_page(self, current_page: int) -> bool:
        """다음 페이지로 이동"""
        try:
            # 현재 URL 수정하여 다음 페이지로 이동
            current_url = self.driver.current_url
            
            # pageIdx 파라미터 업데이트
            if 'pageIdx=' in current_url:
                next_url = re.sub(r'pageIdx=\d+', f'pageIdx={current_page + 1}', current_url)
            else:
                # pageIdx 파라미터 추가
                separator = '&' if '?' in current_url else '?'
                next_url = f"{current_url}{separator}pageIdx={current_page + 1}"
            
            logger.info(f"페이지 {current_page + 1}로 이동: {next_url}")
            self.driver.get(next_url)
            time.sleep(3)  # 페이지 로딩 대기
            
            return True
            
        except Exception as e:
            logger.error(f"페이지 이동 오류: {str(e)}")
            return False
    
    def scrape_category(self, category_name: str, category_url: str, max_pages: int = 5) -> Dict[str, Any]:
        """카테고리 크롤링 메인 함수"""
        start_time = time.time()
        
        try:
            logger.info(f"카테고리 크롤링 시작: {category_name}")
            
            # WebDriver 초기화
            if not self.setup_driver():
                return {
                    'status': 'error',
                    'error_message': 'WebDriver 초기화 실패',
                    'category_name': category_name
                }
            
            # 총 페이지 수 계산
            total_pages = self.get_total_pages(category_url, max_pages)
            
            # 전체 상품 데이터 수집
            all_products = []
            pages_scraped = 0
            
            for page_num in range(1, total_pages + 1):
                try:
                    logger.info(f"페이지 {page_num}/{total_pages} 크롤링 중...")
                    
                    if page_num == 1:
                        # 첫 페이지는 이미 로드됨
                        pass
                    else:
                        # 다음 페이지로 이동
                        if not self.navigate_to_next_page(page_num - 1):
                            logger.error(f"페이지 {page_num} 이동 실패")
                            break
                    
                    # 현재 페이지에서 상품 추출
                    products = self.extract_products_from_page(page_num)
                    
                    if products:
                        # 카테고리 정보 추가
                        for product in products:
                            product['category'] = category_name
                        
                        all_products.extend(products)
                        pages_scraped = page_num
                        logger.info(f"페이지 {page_num}: {len(products)}개 상품 수집")
                    else:
                        logger.warning(f"페이지 {page_num}: 상품을 찾을 수 없음")
                    
                    # 페이지 간 딜레이
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"페이지 {page_num} 크롤링 오류: {str(e)}")
                    continue
            
            # 데이터 검증 및 저장
            if all_products:
                df = pd.DataFrame(all_products)
                
                # GCS 저장 경로 생성
                current_date = datetime.now()
                gcs_path = self.create_gcs_path(category_name, current_date)
                
                # GCS에 업로드
                upload_success = self.gcs_uploader.upload_dataframe_as_csv(df, gcs_path)
                
                elapsed_time = time.time() - start_time
                
                if upload_success:
                    result = {
                        'status': 'success',
                        'category_name': category_name,
                        'products_count': len(all_products),
                        'pages_scraped': pages_scraped,
                        'gcs_path': f"gs://{self.gcs_bucket}/{gcs_path}",
                        'elapsed_time': round(elapsed_time, 2),
                        'scraped_at': datetime.now().isoformat(),
                        'message': f"{category_name} 카테고리 크롤링 완료"
                    }
                else:
                    result = {
                        'status': 'error',
                        'error_message': 'GCS 업로드 실패',
                        'category_name': category_name,
                        'products_count': len(all_products),
                        'elapsed_time': round(elapsed_time, 2)
                    }
            else:
                result = {
                    'status': 'error',
                    'error_message': '수집된 상품이 없음',
                    'category_name': category_name,
                    'pages_scraped': pages_scraped,
                    'elapsed_time': round(time.time() - start_time, 2)
                }
            
            logger.info(f"카테고리 크롤링 완료: {category_name} - {result['status']}")
            return result
            
        except Exception as e:
            error_msg = f"카테고리 크롤링 예외: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'error_message': error_msg,
                'category_name': category_name,
                'elapsed_time': round(time.time() - start_time, 2)
            }
        
        finally:
            # WebDriver 정리
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("WebDriver 정리 완료")
                except:
                    pass
    
    def create_gcs_path(self, category_name: str, date: datetime) -> str:
        """GCS 저장 경로 생성 - 카테고리/년/월/일/파일명.csv"""
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        filename = f"{category_name}_{date.strftime('%Y%m%d')}.csv"
        
        return f"olive-young/{category_name}/{year}/{month}/{day}/{filename}"