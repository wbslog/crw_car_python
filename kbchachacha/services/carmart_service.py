"""
카마트(오토비긴즈) API 연동 서비스
"""

from __future__ import annotations
import json
import time
from typing import Optional
from selenium import webdriver
from selenium.common.exceptions import TimeoutException

from config.settings import crawler_config
from models.car_info import CarInfo
from utils.parser import HtmlParser
from utils.logger import get_logger

logger = get_logger()


class CarmartService:
    """카마트 API 연동 서비스"""
    
    def __init__(self):
        self.parser = HtmlParser()
    
    def sync_car_info(self, driver: webdriver.Chrome, car_info: CarInfo) -> CarInfo:
        """
        카마트 API로 차량 정보 동기화
        
        Args:
            driver: WebDriver 인스턴스
            car_info: 차량 정보 객체
            
        Returns:
            업데이트된 차량 정보
        """
        if not car_info.plate_number:
            logger.warning(f"차량번호 없음 - CAR_ID: {car_info.car_id}")
            car_info.mark_as_sync_failed("차량번호 없음")
            return car_info
        
        try:
            # API URL 생성
            api_url = crawler_config.CARINFO_SYNC_URL.replace(
                "CAR_PLATE_NUMBER", 
                car_info.plate_number
            )
            
            logger.info(f"카마트 API 호출: {car_info.plate_number}")
            
            # API 호출
            driver.get(api_url)
            driver.set_page_load_timeout(5)
            driver.implicitly_wait(1)
            
            # HTML 응답 가져오기
            html = driver.page_source
            
            # HTML 래퍼 제거 및 정규화
            html = self.parser.clean_html_wrapper(html)
            html = self.parser.normalize_wheel_size(html)
            
            # JSON 파싱
            response_data = json.loads(html)
            
            # 성공 여부 확인
            if response_data.get("resultCode") == "00":
                result_data = response_data.get("resultData", {})
                
                # 차량 정보 업데이트
                car_info.update_from_carmart(result_data)
                car_info.mark_as_carmart_synced()
                
                logger.info(
                    f"카마트 동기화 성공 - AP_MODEL_ID: {car_info.ap_model_id}, "
                    f"MODEL: {car_info.model}, PLATE: {car_info.plate_number}"
                )
            else:
                # 동기화 실패
                car_info.clear_carmart_data()
                car_info.mark_as_sync_failed("CAR-MART Sync Fail(Not Found)")
                
                logger.warning(f"카마트 미발견 - PLATE: {car_info.plate_number}")
        
        except TimeoutException:
            logger.warning(f"카마트 API 타임아웃 - PLATE: {car_info.plate_number}")
            car_info.mark_as_sync_failed("API Timeout")
            raise
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 실패 - PLATE: {car_info.plate_number}: {str(e)}")
            car_info.mark_as_sync_failed("JSON Parse Error")
        
        except Exception as e:
            logger.error(f"카마트 동기화 오류 - PLATE: {car_info.plate_number}: {str(e)}")
            car_info.mark_as_sync_failed(f"Error: {str(e)[:50]}")
        
        return car_info