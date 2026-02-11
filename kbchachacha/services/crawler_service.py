"""
크롤러 메인 서비스
"""

from __future__ import annotations
import time
import random
from typing import List
from datetime import datetime

from config.settings import crawler_config, site_config
from config.database import get_db_connection, execute_query
from models.car_info import CarInfo
from services.webdriver_service import WebDriverService
from services.carmart_service import CarmartService
from utils.parser import HtmlParser
from utils.logger import get_logger

logger = get_logger()


class CrawlerService:
    """크롤러 메인 서비스"""
    
    def __init__(self):
        self.webdriver_service = WebDriverService()
        self.carmart_service = CarmartService()
        self.parser = HtmlParser()
        self.start_time = datetime.now()
    
    def get_target_cars(self, limit: int = None) -> List[dict]:
        """
        크롤링 대상 차량 목록 조회
        
        Args:
            limit: 조회 건수 제한
            
        Returns:
            차량 정보 리스트
        """
        if limit is None:
            limit = crawler_config.BATCH_SIZE
        
        query = f"""
            SELECT 
                PRD_SEQ,
                CAR_ID,
                DETAIL_URL,
                SITE_CODE,
                AP_MODEL_ID
            FROM TBL_CAR_PRODUCT_LIST
            WHERE SITE_CODE = '{site_config.KB_CHACHACHA}'
            AND STATUS = '1'
            AND SYNC_STATUS = '1'
            ORDER BY ADD_DATE ASC 
            LIMIT {limit}
        """
        
        
        result = execute_query(query)
        
        logger.info(f"크롤링 대상 차량: {len(result)}건")
        
        return result
    
    def update_car_info(self, car_info: CarInfo, update_type: str = "full"):
        """
        차량 정보 DB 업데이트
        
        Args:
            car_info: 차량 정보
            update_type: 업데이트 타입 ('full' 또는 'basic')
        """
        with get_db_connection() as (conn, cursor):
            try:
                if update_type == "basic":
                    # 기본 정보만 업데이트 (AP_MODEL_ID가 이미 있는 경우)
                    query = """
                        UPDATE TBL_CAR_PRODUCT_LIST
                        SET
                            MOD_YMD = DATE_FORMAT(NOW(), '%%Y%%m%%d'),
                            MOD_HOUR = DATE_FORMAT(NOW(), '%%H'),
                            MOD_DATE = NOW(),
                            FUEL = %s,
                            PLATE_NUMBER = %s,
                            SYNC_STATUS = %s,
                            SYNC_TEXT = %s
                        WHERE STATUS = '1'
                        AND SITE_CODE = %s
                        AND CAR_ID = %s
                        AND PRD_SEQ = %s
                    """
                    
                    params = (
                        car_info.fuel,
                        car_info.plate_number,
                        car_info.sync_status,
                        car_info.sync_text,
                        car_info.site_code,
                        car_info.car_id,
                        car_info.prd_seq
                    )
                    
                    logger.info(
                        f"UPDATE[Find ApModelId] - CAR_ID: {car_info.car_id}, "
                        f"PLATE: {car_info.plate_number}, AP_MODEL_ID: {car_info.ap_model_id}"
                    )
                
                else:
                    # 전체 정보 업데이트
                    query = """
                        UPDATE TBL_CAR_PRODUCT_LIST
                        SET
                            MOD_YMD = DATE_FORMAT(NOW(), '%%Y%%m%%d'),
                            MOD_HOUR = DATE_FORMAT(NOW(), '%%H'),
                            MOD_DATE = NOW(),
                            AP_MODEL_ID = %s,
                            COLOR = %s,
                            NEW_PRICE = %s,
                            MAKE_PRICE = %s,
                            VIN_NUMBER = %s,
                            KIND = %s,
                            DOMESTIC = %s,
                            MAKER = %s,
                            MODEL = %s,
                            MODEL_DETAIL = %s,
                            GRADE = %s,
                            GRADE_DETAIL = %s,
                            FUEL = %s,
                            MISSION = %s,
                            SYNC_STATUS = %s,
                            SYNC_TEXT = %s,
                            FULL_NAME = %s,
                            PLATE_NUMBER = %s
                        WHERE STATUS = '1'
                        AND SITE_CODE = %s
                        AND CAR_ID = %s
                        AND PRD_SEQ = %s
                    """
                    
                    params = (
                        car_info.ap_model_id,
                        car_info.color,
                        car_info.new_price,
                        car_info.make_price,
                        car_info.vin_number,
                        car_info.kind,
                        car_info.domestic,
                        car_info.maker,
                        car_info.model,
                        car_info.model_detail,
                        car_info.grade,
                        car_info.grade_detail,
                        car_info.fuel,
                        car_info.mission,
                        car_info.sync_status,
                        car_info.sync_text,
                        car_info.full_name,
                        car_info.plate_number,
                        car_info.site_code,
                        car_info.car_id,
                        car_info.prd_seq
                    )
                    
                    logger.info(
                        f"UPDATE[CarmartSync] - CAR_ID: {car_info.car_id}, "
                        f"PLATE: {car_info.plate_number}, AP_MODEL_ID: {car_info.ap_model_id}, "
                        f"FULL_NAME: {car_info.full_name}"
                    )
                #print(str(car_info))
                
                cursor.execute(query, params)
                
            except Exception as e:
                logger.error(f"DB 업데이트 오류 - CAR_ID: {car_info.car_id}: {str(e)}")
                raise
    
    def update_closed_car(self, car_info: CarInfo):
        """
        종료된 차량 정보 업데이트
        
        Args:
            car_info: 차량 정보
        """
        with get_db_connection() as (conn, cursor):
            try:
                query = """
                    UPDATE TBL_CAR_PRODUCT_LIST
                    SET
                        MOD_YMD = DATE_FORMAT(NOW(), '%%Y%%m%%d'),
                        MOD_HOUR = DATE_FORMAT(NOW(), '%%H'),
                        MOD_DATE = NOW(),
                        SYNC_STATUS = '9',
                        SYNC_TEXT = %s
                    WHERE STATUS = '1'
                    AND SITE_CODE = %s
                    AND CAR_ID = %s
                    AND PRD_SEQ = %s
                """
                
                params = (
                    car_info.sync_text,
                    car_info.site_code,
                    car_info.car_id,
                    car_info.prd_seq
                )
                
                cursor.execute(query, params)
                
                logger.info(
                    f"UPDATE[Close/SoldOut] - CAR_ID: {car_info.car_id}, "
                    f"URL: {car_info.detail_url}"
                )
                
            except Exception as e:
                logger.error(f"종료 차량 업데이트 오류 - CAR_ID: {car_info.car_id}: {str(e)}")
                raise
    
    def process_car_detail(self, driver, car_info: CarInfo) -> bool:
        """
        차량 상세 정보 처리
        
        Args:
            driver: WebDriver 인스턴스
            car_info: 차량 정보
            
        Returns:
            처리 성공 여부
        """
        # 상세 페이지 크롤링
        html = self.webdriver_service.fetch_page_with_retry(driver, car_info.detail_url)
        
        if not html:
            logger.error(f"페이지 로딩 실패 - CAR_ID: {car_info.car_id}")
            return False
        
        # 로봇 확인 체크
        if "로봇여부 확인" in html:
            wait_sec = random.randint(6, 8)
            logger.warning(f"로봇 감지 - {wait_sec}초 대기")
            time.sleep(wait_sec)
            return False
        
        # 차량 존재 여부 체크
        if "원하시는 페이지를 찾을 수 없습니다" in html:
            logger.warning(f"차량 미존재(삭제) - CAR_ID: {car_info.car_id}")
            car_info.mark_as_closed("차량이 존재하지않음(삭제됨)")
            self.update_closed_car(car_info)
            return True
        
        if "판매가 완료된 차량입니다" in html:
            logger.warning(f"판매 완료 - CAR_ID: {car_info.car_id}")
            car_info.mark_as_closed("판매가 완료된 차량입니다")
            self.update_closed_car(car_info)
            return True
        
        # 차량 정보 파싱
        full_name, years, fuel, plate_number = self.parser.parse_og_description(html)
        
        if not plate_number:
            logger.warning(f"차량번호 파싱 실패 - CAR_ID: {car_info.car_id}")
            return False
        
        # 차량 정보 업데이트
        car_info.full_name = full_name or ""
        car_info.years = years or ""
        car_info.fuel = fuel or ""
        car_info.plate_number = plate_number
        
        # AP_MODEL_ID 이미 있는 경우
        if car_info.ap_model_id:
            car_info.mark_as_completed()
            self.update_car_info(car_info, update_type="basic")
        else:
            # 카마트 동기화 필요
            car_info = self.carmart_service.sync_car_info(driver, car_info)
            self.update_car_info(car_info, update_type="full")
        
        return True
    
    def run(self):
        """크롤러 실행"""
        logger.info("=" * 80)
        logger.info("크롤러 시작")
        logger.info("=" * 80)
        
        driver = None
        
        try:
            # WebDriver 초기화
            driver = self.webdriver_service.initialize()
            
            # 대상 차량 목록 조회
            target_cars = self.get_target_cars()
            total_count = len(target_cars)
            
            if total_count == 0:
                logger.info("처리할 차량이 없습니다")
                return
            
            processed_count = 0
            page_block_count = 0
            
            # 차량별 처리
            for idx, row in enumerate(target_cars, 1):
                try:
                    # CarInfo 객체 생성
                    car_info = CarInfo(
                        prd_seq=str(row['PRD_SEQ']),
                        site_code=site_config.KB_CHACHACHA,
                        car_id=str(row['CAR_ID']),
                        detail_url=row['DETAIL_URL'],
                        ap_model_id=row.get('AP_MODEL_ID') or ""
                    )
                    
                    # 일정 간격마다 재연결
                    page_block_count += 1
                    if page_block_count > crawler_config.RECONNECT_INTERVAL:
                        logger.info(f"재연결 수행 ({page_block_count}건 처리 후)")
                        
                        # 드라이버 재생성
                        self.webdriver_service.close()
                        driver = self.webdriver_service.initialize()
                        
                        page_block_count = 0
                        
                        # 경과 시간 로깅
                        elapsed = self._get_elapsed_time()
                        logger.info(f"경과 시간: {elapsed:.2f}분")
                    
                    # 차량 처리
                    success = self.process_car_detail(driver, car_info)
                    
                    if success:
                        processed_count += 1
                    
                    # 진척도 로깅
                    progress = round(processed_count / total_count * 100, 1)
                    elapsed = self._get_elapsed_time()
                    
                    logger.info("-" * 80)
                    logger.info(
                        f"진행: {processed_count}/{total_count} ({progress}%) | "
                        f"경과시간: {elapsed:.2f}분"
                    )
                    logger.info("-" * 80)
                    
                    # 랜덤 대기
                    wait_sec = random.randint(
                        crawler_config.MIN_SLEEP_SEC,
                        crawler_config.MAX_SLEEP_SEC
                    )
                    logger.debug(f"대기: {wait_sec}초")
                    time.sleep(wait_sec)
                
                except Exception as e:
                    logger.error(f"차량 처리 오류 - CAR_ID: {row['CAR_ID']}: {str(e)}")
                    continue
            
            # 완료 로깅
            elapsed = self._get_elapsed_time()
            logger.info("=" * 80)
            logger.info(f"크롤링 완료 - 소요시간: {elapsed:.2f}분")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.critical(f"크롤러 실행 오류: {str(e)}")
            raise
        
        finally:
            # 정리
            if driver:
                self.webdriver_service.close()
    
    def _get_elapsed_time(self) -> float:
        """
        경과 시간 계산 (분 단위)
        
        Returns:
            경과 시간 (분)
        """
        elapsed = datetime.now() - self.start_time
        return elapsed.total_seconds() / 60