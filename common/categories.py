"""
올리브영 카테고리 데이터 관리
39개 카테고리 정보를 중앙에서 관리
"""

# 올리브영 39개 카테고리 데이터
OLIVEYOUNG_CATEGORIES = {
    # 스킨케어 카테고리
    "스킨케어_스킨토너": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=1000001000100130001",
        "max_pages": 5
    },
    "스킨케어_에센스세럼앰플": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010014",
        "max_pages": 5
    },
    "스킨케어_크림": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010015",
        "max_pages": 5
    },
    "스킨케어_로션": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010016",
        "max_pages": 5
    },
    "스킨케어_미스트오일": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010010",
        "max_pages": 5
    },
    "스킨케어_스킨케어세트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010017",
        "max_pages": 5
    },
    
    # 메이크업 카테고리
    "메이크업_베이스메이크업": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010002",
        "max_pages": 5
    },
    "메이크업_아이메이크업": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010003",
        "max_pages": 5
    },
    "메이크업_립메이크업": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010004",
        "max_pages": 5
    },
    "메이크업_네일": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010005",
        "max_pages": 5
    },
    "메이크업_메이크업도구": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010006",
        "max_pages": 5
    },
    "메이크업_메이크업세트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010007",
        "max_pages": 5
    },
    
    # 바디케어 카테고리
    "바디케어_바디워시": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010018",
        "max_pages": 5
    },
    "바디케어_바디로션": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010019",
        "max_pages": 5
    },
    "바디케어_핸드케어": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010020",
        "max_pages": 5
    },
    "바디케어_풋케어": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010021",
        "max_pages": 5
    },
    "바디케어_데오드란트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010022",
        "max_pages": 5
    },
    "바디케어_바디케어세트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010023",
        "max_pages": 5
    },
    
    # 헤어케어 카테고리
    "헤어케어_샴푸": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010024",
        "max_pages": 5
    },
    "헤어케어_컨디셔너": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010025",
        "max_pages": 5
    },
    "헤어케어_트리트먼트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010026",
        "max_pages": 5
    },
    "헤어케어_헤어스타일링": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010027",
        "max_pages": 5
    },
    "헤어케어_헤어컬러": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010028",
        "max_pages": 5
    },
    "헤어케어_헤어케어세트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010029",
        "max_pages": 5
    },
    
    # 향수 카테고리
    "향수_여성향수": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010030",
        "max_pages": 5
    },
    "향수_남성향수": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010031",
        "max_pages": 5
    },
    "향수_유니섹스향수": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010032",
        "max_pages": 5
    },
    "향수_바디미스트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010033",
        "max_pages": 5
    },
    "향수_향수세트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010034",
        "max_pages": 5
    },
    
    # 남성용품 카테고리
    "남성용품_남성스킨케어": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010035",
        "max_pages": 5
    },
    "남성용품_남성바디케어": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010036",
        "max_pages": 5
    },
    "남성용품_남성헤어케어": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010037",
        "max_pages": 5
    },
    "남성용품_쉐이빙": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010038",
        "max_pages": 5
    },
    "남성용품_남성세트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010039",
        "max_pages": 5
    },
    
    # 건강식품 카테고리
    "건강식품_비타민": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010040",
        "max_pages": 5
    },
    "건강식품_다이어트": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010041",
        "max_pages": 5
    },
    "건강식품_프로틴": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010042",
        "max_pages": 5
    },
    "건강식품_건강차": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010043",
        "max_pages": 5
    },
    
    # 생활용품 카테고리
    "생활용품_세정용품": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010044",
        "max_pages": 5
    },
    "생활용품_구강용품": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010045",
        "max_pages": 5
    },
    "생활용품_마스크": {
        "url": "https://www.oliveyoung.co.kr/store/display/getMCategoryList.do?dispCatNo=100000100010046",
        "max_pages": 5
    }
}

def get_category_list():
    """카테고리 목록 반환"""
    return list(OLIVEYOUNG_CATEGORIES.keys())

def get_category_info(category_name):
    """특정 카테고리 정보 반환"""
    return OLIVEYOUNG_CATEGORIES.get(category_name)

def get_total_categories():
    """전체 카테고리 수 반환"""
    return len(OLIVEYOUNG_CATEGORIES)

def create_safe_task_id(category_name: str) -> str:
    """카테고리 이름을 Airflow task_id에 사용 가능한 형태로 변환"""
    # 특수문자 제거 및 언더스코어로 변환
    safe_name = category_name.replace('/', '_').replace(' ', '_').replace('-', '_')
    # 연속된 언더스코어 제거
    safe_name = '_'.join(filter(None, safe_name.split('_')))
    return safe_name.lower()

def get_gcs_path(category_name: str, date_str: str) -> str:
    """GCS 저장 경로 생성"""
    # 날짜 파싱 (YYYY-MM-DD 형태)
    year, month, day = date_str.split('-')
    
    # 카테고리/년/월/일/파일명 형태로 경로 구성
    return f"{category_name}/{year}/{month}/{day}/{category_name}_{year}{month}{day}.csv"