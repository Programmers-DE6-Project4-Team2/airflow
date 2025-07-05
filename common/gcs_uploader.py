"""
GCS 업로드 공통 유틸리티
"""

import os
import logging
from typing import Optional, Union
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
import pandas as pd
from io import StringIO

logger = logging.getLogger(__name__)

class GCSUploader:
    """Google Cloud Storage 업로드 클래스"""
    
    def __init__(self, bucket_name: str, project_id: Optional[str] = None):
        """
        GCS 업로더 초기화
        
        Args:
            bucket_name: GCS 버킷 이름
            project_id: GCP 프로젝트 ID (선택사항)
        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.client = None
        self.bucket = None
        self._initialize_client()
    
    def _initialize_client(self):
        """GCS 클라이언트 초기화"""
        try:
            if self.project_id:
                self.client = storage.Client(project=self.project_id)
            else:
                self.client = storage.Client()
            
            self.bucket = self.client.bucket(self.bucket_name)
            logger.info(f"GCS 클라이언트 초기화 완료: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"GCS 클라이언트 초기화 실패: {str(e)}")
            raise
    
    def upload_dataframe_as_csv(self, df: pd.DataFrame, gcs_path: str, 
                               encoding: str = 'utf-8-sig') -> bool:
        """
        DataFrame을 CSV 파일로 GCS에 업로드
        
        Args:
            df: 업로드할 DataFrame
            gcs_path: GCS 파일 경로
            encoding: 파일 인코딩 (기본: utf-8-sig)
            
        Returns:
            bool: 업로드 성공 여부
        """
        try:
            # DataFrame을 CSV 문자열로 변환
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, encoding=encoding)
            csv_string = csv_buffer.getvalue()
            
            # GCS에 업로드
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(csv_string, content_type='text/csv')
            
            logger.info(f"CSV 업로드 완료: gs://{self.bucket_name}/{gcs_path}")
            logger.info(f"업로드된 데이터 건수: {len(df)}")
            
            return True
            
        except Exception as e:
            logger.error(f"CSV 업로드 실패: {str(e)}")
            return False
    
    def upload_file(self, local_file_path: str, gcs_path: str) -> bool:
        """
        로컬 파일을 GCS에 업로드
        
        Args:
            local_file_path: 업로드할 로컬 파일 경로
            gcs_path: GCS 파일 경로
            
        Returns:
            bool: 업로드 성공 여부
        """
        try:
            if not os.path.exists(local_file_path):
                logger.error(f"로컬 파일이 존재하지 않음: {local_file_path}")
                return False
            
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_file_path)
            
            logger.info(f"파일 업로드 완료: gs://{self.bucket_name}/{gcs_path}")
            return True
            
        except Exception as e:
            logger.error(f"파일 업로드 실패: {str(e)}")
            return False
    
    def upload_string(self, content: str, gcs_path: str, 
                     content_type: str = 'text/plain') -> bool:
        """
        문자열을 GCS에 업로드
        
        Args:
            content: 업로드할 문자열 내용
            gcs_path: GCS 파일 경로
            content_type: 콘텐츠 타입
            
        Returns:
            bool: 업로드 성공 여부
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(content, content_type=content_type)
            
            logger.info(f"문자열 업로드 완료: gs://{self.bucket_name}/{gcs_path}")
            return True
            
        except Exception as e:
            logger.error(f"문자열 업로드 실패: {str(e)}")
            return False
    
    def file_exists(self, gcs_path: str) -> bool:
        """
        GCS 파일 존재 여부 확인
        
        Args:
            gcs_path: 확인할 GCS 파일 경로
            
        Returns:
            bool: 파일 존재 여부
        """
        try:
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
            
        except Exception as e:
            logger.error(f"파일 존재 확인 실패: {str(e)}")
            return False
    
    def download_file(self, gcs_path: str, local_file_path: str) -> bool:
        """
        GCS 파일을 로컬로 다운로드
        
        Args:
            gcs_path: 다운로드할 GCS 파일 경로
            local_file_path: 저장할 로컬 파일 경로
            
        Returns:
            bool: 다운로드 성공 여부
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.download_to_filename(local_file_path)
            
            logger.info(f"파일 다운로드 완료: gs://{self.bucket_name}/{gcs_path} -> {local_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"파일 다운로드 실패: {str(e)}")
            return False
    
    def list_files(self, prefix: str = "") -> list:
        """
        GCS 버킷의 파일 목록 조회
        
        Args:
            prefix: 파일 경로 접두사
            
        Returns:
            list: 파일 경로 목록
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            file_list = [blob.name for blob in blobs]
            
            logger.info(f"파일 목록 조회 완료: {len(file_list)}개 파일")
            return file_list
            
        except Exception as e:
            logger.error(f"파일 목록 조회 실패: {str(e)}")
            return []
    
    def delete_file(self, gcs_path: str) -> bool:
        """
        GCS 파일 삭제
        
        Args:
            gcs_path: 삭제할 GCS 파일 경로
            
        Returns:
            bool: 삭제 성공 여부
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.delete()
            
            logger.info(f"파일 삭제 완료: gs://{self.bucket_name}/{gcs_path}")
            return True
            
        except Exception as e:
            logger.error(f"파일 삭제 실패: {str(e)}")
            return False
    
    def get_file_info(self, gcs_path: str) -> dict:
        """
        GCS 파일 정보 조회
        
        Args:
            gcs_path: 조회할 GCS 파일 경로
            
        Returns:
            dict: 파일 정보 딕셔너리
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.reload()
            
            file_info = {
                'name': blob.name,
                'size': blob.size,
                'created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'content_type': blob.content_type,
                'etag': blob.etag,
                'exists': blob.exists()
            }
            
            logger.info(f"파일 정보 조회 완료: {gcs_path}")
            return file_info
            
        except Exception as e:
            logger.error(f"파일 정보 조회 실패: {str(e)}")
            return {}


def create_gcs_uploader(bucket_name: str, project_id: Optional[str] = None) -> GCSUploader:
    """
    GCS 업로더 인스턴스 생성 헬퍼 함수
    
    Args:
        bucket_name: GCS 버킷 이름
        project_id: GCP 프로젝트 ID (선택사항)
        
    Returns:
        GCSUploader: 초기화된 업로더 인스턴스
    """
    return GCSUploader(bucket_name, project_id)