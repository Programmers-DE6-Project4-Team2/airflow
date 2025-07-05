"""
데이터 스키마 정의 및 검증 유틸리티
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# 올리브영 상품 데이터 스키마
OLIVEYOUNG_PRODUCT_SCHEMA = {
    'product_id': str,
    'name': str,
    'brand': str,
    'price': str,
    'original_price': str,
    'discount_rate': str,
    'rating': str,
    'review_count': str,
    'url': str,
    'image_url': str,
    'category': str,
    'scraped_at': str,
    'page_number': int
}

# 필수 컬럼 정의
REQUIRED_COLUMNS = [
    'product_id', 'name', 'brand', 'price', 'url', 'scraped_at', 'category'
]

# 선택적 컬럼 정의
OPTIONAL_COLUMNS = [
    'original_price', 'discount_rate', 'rating', 'review_count', 
    'image_url', 'page_number'
]

class DataValidator:
    """데이터 검증 클래스"""
    
    def __init__(self, schema: Dict[str, type] = None):
        """
        데이터 검증기 초기화
        
        Args:
            schema: 데이터 스키마 딕셔너리
        """
        self.schema = schema or OLIVEYOUNG_PRODUCT_SCHEMA
        self.required_columns = REQUIRED_COLUMNS
        self.optional_columns = OPTIONAL_COLUMNS
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        DataFrame 유효성 검사
        
        Args:
            df: 검증할 DataFrame
            
        Returns:
            Dict[str, Any]: 검증 결과
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'missing_required_columns': [],
                'missing_optional_columns': [],
                'extra_columns': [],
                'null_counts': {},
                'data_types': {}
            }
        }
        
        try:
            # 1. 빈 DataFrame 검사
            if df.empty:
                validation_result['is_valid'] = False
                validation_result['errors'].append("DataFrame이 비어있습니다.")
                return validation_result
            
            # 2. 필수 컬럼 검사
            missing_required = [col for col in self.required_columns if col not in df.columns]
            if missing_required:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f"필수 컬럼 누락: {missing_required}")
                validation_result['summary']['missing_required_columns'] = missing_required
            
            # 3. 선택적 컬럼 검사
            missing_optional = [col for col in self.optional_columns if col not in df.columns]
            if missing_optional:
                validation_result['warnings'].append(f"선택적 컬럼 누락: {missing_optional}")
                validation_result['summary']['missing_optional_columns'] = missing_optional
            
            # 4. 추가 컬럼 검사
            expected_columns = set(self.required_columns + self.optional_columns)
            extra_columns = [col for col in df.columns if col not in expected_columns]
            if extra_columns:
                validation_result['warnings'].append(f"예상되지 않은 컬럼: {extra_columns}")
                validation_result['summary']['extra_columns'] = extra_columns
            
            # 5. 데이터 타입 검사
            for column in df.columns:
                if column in self.schema:
                    expected_type = self.schema[column]
                    actual_type = df[column].dtype
                    validation_result['summary']['data_types'][column] = str(actual_type)
                    
                    # 문자열 타입 검사 (object 타입으로 표시됨)
                    if expected_type == str and actual_type != 'object':
                        validation_result['warnings'].append(
                            f"컬럼 '{column}': 예상 타입 {expected_type.__name__}, 실제 타입 {actual_type}"
                        )
            
            # 6. NULL 값 검사
            for column in df.columns:
                null_count = df[column].isnull().sum()
                validation_result['summary']['null_counts'][column] = int(null_count)
                
                if column in self.required_columns and null_count > 0:
                    validation_result['warnings'].append(
                        f"필수 컬럼 '{column}'에 {null_count}개의 NULL 값이 있습니다."
                    )
            
            # 7. 특별 검증 규칙
            validation_result.update(self._validate_special_rules(df))
            
            logger.info(f"데이터 검증 완료: {len(df)}행, 유효성 {validation_result['is_valid']}")
            
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"검증 중 오류 발생: {str(e)}")
            logger.error(f"데이터 검증 오류: {str(e)}")
        
        return validation_result
    
    def _validate_special_rules(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        특별한 검증 규칙 적용
        
        Args:
            df: 검증할 DataFrame
            
        Returns:
            Dict[str, Any]: 추가 검증 결과
        """
        result = {'errors': [], 'warnings': []}
        
        try:
            # URL 형식 검사
            if 'url' in df.columns:
                invalid_urls = df[~df['url'].str.startswith('http', na=False)]
                if not invalid_urls.empty:
                    result['warnings'].append(f"유효하지 않은 URL 형식: {len(invalid_urls)}개")
            
            # 가격 형식 검사
            if 'price' in df.columns:
                # 빈 가격 검사
                empty_prices = df[df['price'].isnull() | (df['price'] == '')]
                if not empty_prices.empty:
                    result['warnings'].append(f"빈 가격 정보: {len(empty_prices)}개")
            
            # 제품 ID 중복 검사
            if 'product_id' in df.columns:
                duplicate_ids = df[df['product_id'].duplicated()]
                if not duplicate_ids.empty:
                    result['warnings'].append(f"중복된 제품 ID: {len(duplicate_ids)}개")
            
            # 날짜 형식 검사
            if 'scraped_at' in df.columns:
                try:
                    # 날짜 형식 확인 (ISO 8601 형식 예상)
                    pd.to_datetime(df['scraped_at'], errors='coerce')
                except:
                    result['warnings'].append("scraped_at 컬럼의 날짜 형식이 올바르지 않습니다.")
            
        except Exception as e:
            result['errors'].append(f"특별 검증 규칙 오류: {str(e)}")
        
        return result
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        DataFrame 정리 및 정규화
        
        Args:
            df: 정리할 DataFrame
            
        Returns:
            pd.DataFrame: 정리된 DataFrame
        """
        cleaned_df = df.copy()
        
        try:
            # 1. 문자열 컬럼 정리
            string_columns = ['name', 'brand', 'price', 'original_price', 'discount_rate', 
                            'rating', 'review_count', 'url', 'image_url', 'category']
            
            for col in string_columns:
                if col in cleaned_df.columns:
                    # 앞뒤 공백 제거
                    cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
                    # 빈 문자열을 NaN으로 변환
                    cleaned_df[col] = cleaned_df[col].replace('', pd.NA)
            
            # 2. 숫자 컬럼 정리
            if 'page_number' in cleaned_df.columns:
                cleaned_df['page_number'] = pd.to_numeric(cleaned_df['page_number'], errors='coerce')
            
            # 3. 날짜 컬럼 정리
            if 'scraped_at' in cleaned_df.columns:
                cleaned_df['scraped_at'] = pd.to_datetime(cleaned_df['scraped_at'], errors='coerce')
                # 다시 문자열로 변환 (ISO 형식)
                cleaned_df['scraped_at'] = cleaned_df['scraped_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # 4. 중복 제거
            if 'product_id' in cleaned_df.columns:
                cleaned_df = cleaned_df.drop_duplicates(subset=['product_id'], keep='first')
            
            logger.info(f"DataFrame 정리 완료: {len(cleaned_df)}행")
            
        except Exception as e:
            logger.error(f"DataFrame 정리 오류: {str(e)}")
        
        return cleaned_df


def create_sample_dataframe() -> pd.DataFrame:
    """
    샘플 DataFrame 생성 (테스트용)
    
    Returns:
        pd.DataFrame: 샘플 데이터
    """
    sample_data = {
        'product_id': ['P001', 'P002', 'P003'],
        'name': ['샘플 제품 1', '샘플 제품 2', '샘플 제품 3'],
        'brand': ['브랜드 A', '브랜드 B', '브랜드 C'],
        'price': ['10,000원', '20,000원', '30,000원'],
        'original_price': ['15,000원', '25,000원', '35,000원'],
        'discount_rate': ['33%', '20%', '14%'],
        'rating': ['4.5', '4.0', '4.8'],
        'review_count': ['100', '50', '200'],
        'url': ['https://example.com/p1', 'https://example.com/p2', 'https://example.com/p3'],
        'image_url': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg', 'https://example.com/img3.jpg'],
        'category': ['스킨케어', '메이크업', '바디케어'],
        'scraped_at': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 3,
        'page_number': [1, 1, 2]
    }
    
    return pd.DataFrame(sample_data)


def validate_oliveyoung_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    올리브영 데이터 검증 헬퍼 함수
    
    Args:
        df: 검증할 DataFrame
        
    Returns:
        Dict[str, Any]: 검증 결과
    """
    validator = DataValidator(OLIVEYOUNG_PRODUCT_SCHEMA)
    return validator.validate_dataframe(df)


def clean_oliveyoung_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    올리브영 데이터 정리 헬퍼 함수
    
    Args:
        df: 정리할 DataFrame
        
    Returns:
        pd.DataFrame: 정리된 DataFrame
    """
    validator = DataValidator(OLIVEYOUNG_PRODUCT_SCHEMA)
    return validator.clean_dataframe(df)