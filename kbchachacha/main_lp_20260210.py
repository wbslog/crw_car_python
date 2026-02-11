"""
KB Chachacha 사이트 수집 기능 - 멀티스레드 슬라이딩 도어 버전
First Make Date: 2025.10.29
Optimized Date: 2026.01.16
Multi-thread Sliding Door Version: 2026.01.16

주요 개선사항:
- 10개 스레드로 병렬 처리
- 슬라이딩 도어 방식: 각 스레드가 50페이지 처리 후 다음 구간으로 이동
  예) Thread 1: 1~50 -> 501~550 -> 1001~1050...
      Thread 2: 51~100 -> 551~600 -> 1051~1100...
- "입력하신 정보에 맞는 차량이 없습니다" 감지 시 스레드 종료
- 스레드별 독립적인 DB 연결 및 웹드라이버 관리
"""

import os
import time
import random
import re
import traceback
from datetime import datetime
from contextlib import contextmanager
from typing import Dict, List, Generator, Optional
import threading
from queue import Queue

import pymysql
from pymysql.cursors import DictCursor
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

import varList


class CrawlerConfig:
    """크롤러 설정 클래스"""
    
    # DB 설정
    DB_CONFIG = {
        'user': varList.dbUserId,
        'passwd': varList.dbUserPass,
        'host': varList.dbServerHost,
        'port': varList.dbServerPort,
        'db': varList.dbServerName,
        'charset': 'utf8mb4',
        'cursorclass': DictCursor
    }
    
    # 프록시 설정
    PROXY_USER_ID = varList.proxyUserId
    PROXY_USER_PASS = varList.proxyUserPass
    
    # 크롬 드라이버 설정
    CHROME_DRIVER_FILE = varList.CHROME_DRIVER_FILE_PATH
    USER_AGENT = varList.USER_AGENT_NAME
    
    # URL 설정
    INDEX_URL = varList.INDEX_SAMPLE_URL
    LP_URL_TEMPLATE = varList.TARGET_SITE_LP_URL
    MAIN_DETAIL_URL = varList.MAIN_DETAIL_URL
    MAIN_INDEX_HOST = varList.MAIN_INDEX_HOST_NAME
    MAIN_HOST = varList.MAIN_HOST_NAME
    
    # 크롤링 설정
    SITE_CODE = "2000"  # KBchachacha
    PAGE_RECONNECT_INTERVAL = 5  # 5페이지마다 재연결
    BATCH_SIZE = 50  # DB 배치 처리 크기
    MIN_SLEEP = 8
    MAX_SLEEP = 13
    
    # 멀티스레드 설정
    THREAD_COUNT = 2  # 스레드 개수
    PAGES_PER_THREAD = 50  # 스레드당 한 번에 처리할 페이지 수
    
    LOG_DIR = "/data/niffler_kb/logs"
    LOG_FILE_PREFIX = "crawler_lp_"
    
    # 종료 체크 문구
    NO_RESULT_TEXT = "입력하신 정보에 맞는 차량이 없습니다"


class ThreadSafeLogger:
    """스레드 안전 로깅 유틸리티"""
    
    _log_file = None
    _log_file_path = None
    _lock = threading.Lock()
    
    @classmethod
    def initialize(cls):
        """로거 초기화 - 로그 파일 생성"""
        try:
            # 로그 디렉토리 생성
            os.makedirs(CrawlerConfig.LOG_DIR, exist_ok=True)
            
            # 로그 파일명 생성
            today = datetime.now().strftime("%Y%m%d")
            log_filename = f"{CrawlerConfig.LOG_FILE_PREFIX}{today}.log"
            cls._log_file_path = os.path.join(CrawlerConfig.LOG_DIR, log_filename)
            
            # 로그 파일 열기 (append 모드)
            cls._log_file = open(cls._log_file_path, 'a', encoding='utf-8')
            
            cls.log(f"로그 파일 초기화: {cls._log_file_path}")
            
        except Exception as e:
            print(f"로그 파일 초기화 실패: {e}")
            cls._log_file = None
    
    @classmethod
    def log(cls, msg: str, thread_id: int = None) -> None:
        """스레드 안전 로그 출력 (콘솔 + 파일)"""
        with cls._lock:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            thread_prefix = f"[T{thread_id:02d}]" if thread_id is not None else "[MAIN]"
            log_message = f"## [{timestamp}] {thread_prefix} [{msg}]"
            
            # 콘솔 출력
            print(log_message)
            
            # 파일 출력
            if cls._log_file:
                try:
                    cls._log_file.write(log_message + '\n')
                    cls._log_file.flush()
                except Exception as e:
                    print(f"로그 파일 쓰기 실패: {e}")
    
    @classmethod
    def close(cls):
        """로그 파일 닫기"""
        with cls._lock:
            if cls._log_file:
                try:
                    cls.log("로그 파일 종료")
                    cls._log_file.close()
                    cls._log_file = None
                except Exception as e:
                    print(f"로그 파일 닫기 실패: {e}")


class PageAllocator:
    """스레드별 페이지 할당 관리 클래스 (슬라이딩 도어 방식)"""
    
    def __init__(self, thread_count: int, pages_per_thread: int):
        self.thread_count = thread_count
        self.pages_per_thread = pages_per_thread
        self.lock = threading.Lock()
        
    def get_page_range(self, thread_id: int, round_num: int) -> tuple:
        """
        특정 스레드의 특정 라운드에서 처리할 페이지 범위 계산
        
        Args:
            thread_id: 스레드 ID (1부터 시작)
            round_num: 라운드 번호 (0부터 시작)
        
        Returns:
            (start_page, end_page) 튜플
        
        예시:
            Thread 1, Round 0: (1, 50)
            Thread 1, Round 1: (501, 550)
            Thread 1, Round 2: (1001, 1050)
            
            Thread 2, Round 0: (51, 100)
            Thread 2, Round 1: (551, 600)
            Thread 2, Round 2: (1051, 1100)
        """
        # 기본 오프셋: (thread_id - 1) * pages_per_thread
        base_offset = (thread_id - 1) * self.pages_per_thread
        
        # 라운드별 오프셋: round_num * (thread_count * pages_per_thread)
        round_offset = round_num * (self.thread_count * self.pages_per_thread)
        
        start_page = base_offset + round_offset + 1
        end_page = start_page + self.pages_per_thread - 1
        
        return start_page, end_page


class DatabaseManager:
    """데이터베이스 관리 클래스"""
    
    _proxy_lock = threading.Lock()  # 프록시 조회용 락
    
    def __init__(self, config: Dict):
        self.config = config
    
    def get_connection(self):
        """DB 연결 생성"""
        try:
            connection = pymysql.connect(**self.config)
            return connection
        except Exception as e:
            raise Exception(f"DB 연결 실패: {e}")
    
    @contextmanager
    def get_connection_context(self):
        """DB 연결 컨텍스트 매니저"""
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            yield cursor
        except Exception as e:
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.commit()
                connection.close()
    
    def get_proxy_info(self, thread_id: int) -> Dict:
        """사용 빈도가 낮은 프록시 정보 조회 (스레드 안전)"""
        with self._proxy_lock:
            with self.get_connection_context() as cursor:
                query = """
                    SELECT PROXY_CODE, PROXY_IP, PROXY_PORT, EXEC_COUNT
                    FROM NIF_PROXY_LIST 
                    WHERE STATUS = '1'
                    ORDER BY EXEC_COUNT ASC
                    LIMIT 1
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
                if not result:
                    raise Exception("사용 가능한 프록시가 없습니다")
                
                # 실행 카운트 증가
                update_query = """
                    UPDATE NIF_PROXY_LIST
                    SET EXEC_COUNT = EXEC_COUNT + 1
                    WHERE PROXY_CODE = %s
                """
                cursor.execute(update_query, (result['PROXY_CODE'],))
                
                proxy_info = {
                    'PROXY_CODE': result['PROXY_CODE'],
                    'PROXY_IP': result['PROXY_IP'],
                    'PROXY_PORT': result['PROXY_PORT'],
                    'PROXY_USER_ID': CrawlerConfig.PROXY_USER_ID,
                    'PROXY_USER_PASS': CrawlerConfig.PROXY_USER_PASS
                }
                
                ThreadSafeLogger.log(
                    f"프록시 정보: IP={proxy_info['PROXY_IP']}, PORT={proxy_info['PROXY_PORT']}", 
                    thread_id
                )
                return proxy_info
    
    def get_car_info(self, car_basic: Dict, cursor) -> Dict:
        """차량 상세 정보 조회"""
        query = """
            SELECT 
                DOMESTIC, KIND, MAKER, MODEL, MODEL_DETAIL,
                GRADE, GRADE_DETAIL, COLOR, MISSION, AP_MODEL_ID,
                NEW_PRICE, MAKE_PRICE
            FROM TBL_CAR_PRODUCT_LIST
            WHERE SITE_CODE = %s
              AND FULL_NAME = %s
              AND YEARS = %s
              AND AP_MODEL_ID IS NOT NULL
              AND MAKER IS NOT NULL
            ORDER BY ADD_DATE DESC
            LIMIT 1
        """
        cursor.execute(query, (
            car_basic['siteCode'],
            car_basic['fullName'],
            car_basic['years']
        ))
        result = cursor.fetchone()
        
        car_info = car_basic.copy()
        
        if result:
            car_info.update({
                'domestic': result.get('DOMESTIC') or '',
                'kind': result.get('KIND') or '',
                'maker': result.get('MAKER') or '',
                'model': result.get('MODEL') or '',
                'modelDetail': result.get('MODEL_DETAIL') or '',
                'grade': result.get('GRADE') or '',
                'gradeDetail': result.get('GRADE_DETAIL') or '',
                'color': result.get('COLOR') or '',
                'mission': result.get('MISSION') or '',
                'apModelId': result.get('AP_MODEL_ID') or '',
                'newPrice': result.get('NEW_PRICE') or '',
                'makePrice': result.get('MAKE_PRICE') or ''
            })
        else:
            empty_fields = [
                'domestic', 'kind', 'maker', 'model', 'modelDetail',
                'grade', 'gradeDetail', 'color', 'mission', 'apModelId',
                'newPrice', 'makePrice'
            ]
            for field in empty_fields:
                car_info[field] = ''
        
        return car_info
    
    def upsert_car_batch(self, car_list: List[Dict], cursor, thread_id: int) -> tuple:
        """차량 정보 배치 저장/업데이트"""
        insert_count = 0
        update_count = 0
        
        for car_info in car_list:
            try:
                check_query = """
                    SELECT COUNT(*) AS CNT 
                    FROM TBL_CAR_PRODUCT_LIST 
                    WHERE STATUS = '1'
                      AND SYNC_STATUS = '1'
                      AND SITE_CODE = %s
                      AND CAR_ID = %s
                """
                cursor.execute(check_query, (car_info['siteCode'], car_info['carId']))
                result = cursor.fetchone()
                
                if result['CNT'] == 0:
                    # INSERT
                    insert_query = """
                        INSERT INTO TBL_CAR_PRODUCT_LIST (
                            SITE_CODE, CAR_ID, MODEL_DETAIL_ORI, YEARS, FIRST_DATE,
                            KM, PRICE, STATUS, ADD_DATE, ADD_YMD, ADD_HOUR,
                            FULL_NAME, DETAIL_URL, SYNC_STATUS, SYNC_TEXT,
                            DOMESTIC, KIND, MAKER, MODEL, MODEL_DETAIL,
                            GRADE, GRADE_DETAIL, COLOR, MISSION, AP_MODEL_ID,
                            NEW_PRICE, MAKE_PRICE
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, '1', NOW(),
                            DATE_FORMAT(NOW(), '%%Y%%m%%d'),
                            DATE_FORMAT(NOW(), '%%H'),
                            %s, %s, '1', 'LP수집완료',
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    cursor.execute(insert_query, (
                        car_info['siteCode'], car_info['carId'],
                        car_info['modelDetailOri'], car_info['years'],
                        car_info['firstDate'], car_info['km'], car_info['price'],
                        car_info['fullName'], car_info['detailUrl'],
                        car_info['domestic'], car_info['kind'], car_info['maker'],
                        car_info['model'], car_info['modelDetail'], car_info['grade'],
                        car_info['gradeDetail'], car_info['color'], car_info['mission'],
                        car_info['apModelId'], car_info['newPrice'], car_info['makePrice']
                    ))
                    insert_count += 1
                    ThreadSafeLogger.log(
                        f"INSERT - carId:{car_info['carId']} | apModelId:{car_info['apModelId']} | "
                        f"fullName:{car_info['fullName']} | detailUrl:{car_info['detailUrl']}", 
                        thread_id
                    )
                else:
                    # UPDATE
                    update_query = """
                        UPDATE TBL_CAR_PRODUCT_LIST
                        SET MODEL_DETAIL_ORI = %s, PRICE = %s, YEARS = %s,
                            KM = %s, FULL_NAME = %s,
                            MOD_YMD = DATE_FORMAT(NOW(), '%%Y%%m%%d'),
                            MOD_HOUR = DATE_FORMAT(NOW(), '%%H'),
                            FIRST_DATE = %s, DETAIL_URL = %s,
                            DOMESTIC = %s, KIND = %s, MODEL = %s,
                            MODEL_DETAIL = %s, GRADE = %s, GRADE_DETAIL = %s,
                            COLOR = %s, MISSION = %s, AP_MODEL_ID = %s,
                            NEW_PRICE = %s, MAKE_PRICE = %s
                        WHERE STATUS = '1'
                          AND SYNC_STATUS = '1'
                          AND SITE_CODE = %s
                          AND CAR_ID = %s
                    """
                    cursor.execute(update_query, (
                        car_info['modelDetailOri'], car_info['price'], car_info['years'],
                        car_info['km'], car_info['fullName'], car_info['firstDate'],
                        car_info['detailUrl'], car_info['domestic'], car_info['kind'],
                        car_info['model'], car_info['modelDetail'], car_info['grade'],
                        car_info['gradeDetail'], car_info['color'], car_info['mission'],
                        car_info['apModelId'], car_info['newPrice'], car_info['makePrice'],
                        car_info['siteCode'], car_info['carId']
                    ))
                    update_count += 1
                    ThreadSafeLogger.log(
                        f"UPDATE - carId:{car_info['carId']} | apModelId:{car_info['apModelId']} | "
                        f"fullName:{car_info['fullName']} | detailUrl:{car_info['detailUrl']}", 
                        thread_id
                    )
            
            except Exception as e:
                ThreadSafeLogger.log(f"DB 저장 실패 - carId:{car_info.get('carId')}: {e}", thread_id)
                continue
        
        return insert_count, update_count


class WebDriverManager:
    """웹드라이버 관리 클래스"""
    
    def __init__(self, db_manager: DatabaseManager, thread_id: int):
        self.db_manager = db_manager
        self.thread_id = thread_id
        self.driver = None
    
    def create_driver(self) -> webdriver.Chrome:
        """프록시 설정된 웹드라이버 생성"""
        proxy_info = self.db_manager.get_proxy_info(self.thread_id)
        
        # Proxy 설정
        wire_options = {
            'proxy': {
                'http': f"socks5://{proxy_info['PROXY_USER_ID']}:{proxy_info['PROXY_USER_PASS']}@{proxy_info['PROXY_IP']}:{proxy_info['PROXY_PORT']}",
                'https': f"socks5://{proxy_info['PROXY_USER_ID']}:{proxy_info['PROXY_USER_PASS']}@{proxy_info['PROXY_IP']}:{proxy_info['PROXY_PORT']}",
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        
        # Chrome 옵션 설정
        chrome_options = Options()
        chrome_options.add_argument(f"--user-agent={CrawlerConfig.USER_AGENT}")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        
        service = Service(CrawlerConfig.CHROME_DRIVER_FILE)
        driver = webdriver.Chrome(
            options=chrome_options,
            service=service,
            seleniumwire_options=wire_options
        )
        
        ThreadSafeLogger.log("웹드라이버 생성 완료", self.thread_id)
        return driver
    
    def initialize_session(self) -> webdriver.Chrome:
        """세션 초기화"""
        if self.driver:
            self.driver.quit()
        
        self.driver = self.create_driver()
        
        try:
            self.driver.get(CrawlerConfig.INDEX_URL)
            self.driver.implicitly_wait(20)
            time.sleep(5)
            ThreadSafeLogger.log("세션 초기화 완료", self.thread_id)
        except Exception as e:
            ThreadSafeLogger.log(f"세션 초기화 실패: {e}", self.thread_id)
            raise
        
        return self.driver
    
    def quit(self):
        """드라이버 종료"""
        if self.driver:
            self.driver.quit()
            self.driver = None


class CarDataParser:
    """차량 데이터 파싱 클래스"""
    
    @staticmethod
    def extract_regex(text: str, pattern: str) -> str:
        """정규식으로 데이터 추출"""
        text = text.replace("\n", "").replace("\r", "").replace("\t", "")
        matches = re.search(pattern, text)
        return matches.group(1).strip() if matches else ""
    
    @staticmethod
    def parse_year(year_text: str) -> str:
        """연식 파싱"""
        year = CarDataParser.extract_regex(year_text, r"([0-9]{2})년형")
        if year and len(year) == 2:
            year = "20" + year
        return year
    
    @staticmethod
    def parse_first_date(year_text: str) -> str:
        """최초등록일 파싱"""
        first_date = CarDataParser.extract_regex(year_text, r"([0-9]{2}/[0-9]{2})식")
        if first_date and len(first_date) == 5:
            first_date = "20" + first_date.replace("/", "-") + "-01"
        return first_date
    
    @staticmethod
    def parse_car_item(area) -> Optional[Dict]:
        """차량 항목 파싱"""
        try:
            car_seq = area.get("data-car-seq") or area.get("data-seq")
            if not car_seq:
                return None
            
            html_body = area.decode_contents().strip()
            html_body = html_body.replace("\n", "").replace("\r", "").replace("\t", "")
            
            model_name = area.find("strong", class_="tit")
            if not model_name:
                return None
            model_name = model_name.get_text(strip=True)
            
            price = CarDataParser.extract_regex(
                html_body,
                r'<span class="price">([0-9,]{0,10})<span class="unit">'
            ).replace(",", "").replace(".", "")
            
            km = CarDataParser.extract_regex(
                html_body,
                r'<span>([0-9,]{0,20})km</span>'
            ).replace(",", "").replace(".", "")
            
            year_full_text = CarDataParser.extract_regex(
                html_body,
                r'<div class="data-line"><span>([0-9/식년형()]{0,50})</span>'
            )
            
            year = CarDataParser.parse_year(year_full_text)
            first_date = CarDataParser.parse_first_date(year_full_text)
            detail_url = f"{CrawlerConfig.MAIN_DETAIL_URL}?carSeq={car_seq}"
            
            return {
                'carId': car_seq,
                'siteCode': CrawlerConfig.SITE_CODE,
                'modelDetailOri': model_name,
                'fullName': model_name,
                'years': year,
                'km': km,
                'price': price,
                'firstDate': first_date,
                'detailUrl': detail_url
            }
        
        except Exception as e:
            return None


class CrawlerThread(threading.Thread):
    """크롤러 스레드 클래스 - 슬라이딩 도어 방식"""
    
    def __init__(self, thread_id: int, page_allocator: PageAllocator, stats_queue: Queue):
        super().__init__()
        self.thread_id = thread_id
        self.page_allocator = page_allocator
        self.stats_queue = stats_queue
        
        self.db_manager = DatabaseManager(CrawlerConfig.DB_CONFIG)
        self.driver_manager = WebDriverManager(self.db_manager, thread_id)
        self.parser = CarDataParser()
        
        self.stats = {
            'thread_id': thread_id,
            'rounds': 0,
            'pages': 0,
            'cars': 0,
            'inserts': 0,
            'updates': 0,
            'errors': 0
        }
    
    def check_no_result(self, html: str) -> bool:
        """결과 없음 체크"""
        return CrawlerConfig.NO_RESULT_TEXT in html
    
    def fetch_page(self, page_num: int) -> Optional[str]:
        """페이지 HTML 가져오기"""
        try:
            url = CrawlerConfig.LP_URL_TEMPLATE.replace('VAR_PAGE_NUM', str(page_num))
            self.driver_manager.driver.get(url)
            
            wait = WebDriverWait(self.driver_manager.driver, 30)
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
            )
            
            html = self.driver_manager.driver.page_source
            ThreadSafeLogger.log(f"페이지 {page_num} 로드 완료 > URL: {url}", self.thread_id)
            return html
        
        except TimeoutException:
            ThreadSafeLogger.log(f"페이지 {page_num} 타임아웃", self.thread_id)
            return None
        except Exception as e:
            ThreadSafeLogger.log(f"페이지 {page_num} 로드 실패: {e}", self.thread_id)
            return None
    
    def parse_page(self, html: str, cursor) -> Generator[Dict, None, None]:
        """페이지에서 차량 정보 추출"""
        soup = BeautifulSoup(html, "lxml")
        wrap = soup.select_one("div.list-in.type-wd-list")
        
        if not wrap:
            return
        
        areas = wrap.select("div.area")
        
        for area in areas:
            car_basic = self.parser.parse_car_item(area)
            if car_basic:
                car_info = self.db_manager.get_car_info(car_basic, cursor)
                yield car_info
    
    def process_round(self, round_num: int, conn, cursor) -> bool:
        """
        한 라운드(50페이지) 처리
        
        Returns:
            True: 계속 진행
            False: 종료 신호 (결과 없음 또는 오류)
        """
        start_page, end_page = self.page_allocator.get_page_range(self.thread_id, round_num)
        
        ThreadSafeLogger.log(
            f"Round {round_num + 1} 시작 - 페이지 범위: {start_page} ~ {end_page}", 
            self.thread_id
        )
        
        page_block = 0
        car_batch = []
        round_car_count = 0
        
        for page_num in range(start_page, end_page + 1):
            # 주기적 재연결
            page_block += 1
            if page_block > CrawlerConfig.PAGE_RECONNECT_INTERVAL:
                ThreadSafeLogger.log(f"재연결 - 페이지 {page_num}", self.thread_id)
                page_block = 0
                self.driver_manager.initialize_session()
            
            # 페이지 크롤링
            html = self.fetch_page(page_num)
            if not html:
                ThreadSafeLogger.log(f"페이지 {page_num}를 가져올 수 없습니다.", self.thread_id)
                return False
            
            # "입력하신 정보에 맞는 차량이 없습니다" 체크
            if self.check_no_result(html):
                ThreadSafeLogger.log(
                    f"'{CrawlerConfig.NO_RESULT_TEXT}' 감지 - 스레드 종료", 
                    self.thread_id
                )
                return False
            
            # 차량 정보 파싱
            page_car_count = 0
            for car_info in self.parse_page(html, cursor):
                car_batch.append(car_info)
                page_car_count += 1
                round_car_count += 1
                
                # 배치 저장
                if len(car_batch) >= CrawlerConfig.BATCH_SIZE:
                    insert, update = self.db_manager.upsert_car_batch(
                        car_batch, cursor, self.thread_id
                    )
                    conn.commit()
                    self.stats['inserts'] += insert
                    self.stats['updates'] += update
                    self.stats['cars'] += len(car_batch)
                    car_batch.clear()
            
            self.stats['pages'] += 1
            
            ThreadSafeLogger.log(
                f"페이지 {page_num} 완료 - 차량 {page_car_count}대 | "
                f"Round 누적: {round_car_count}대 | "
                f"전체 누적: P={self.stats['pages']}, C={self.stats['cars']}, "
                f"I={self.stats['inserts']}, U={self.stats['updates']}", 
                self.thread_id
            )
            
            # 차량이 없으면 종료
            if page_car_count == 0:
                ThreadSafeLogger.log("차량 없음 - 스레드 종료", self.thread_id)
                return False
            
            # 랜덤 대기
            sleep_time = random.randint(CrawlerConfig.MIN_SLEEP, CrawlerConfig.MAX_SLEEP)
            time.sleep(sleep_time)
        
        # 남은 배치 처리
        if car_batch:
            insert, update = self.db_manager.upsert_car_batch(
                car_batch, cursor, self.thread_id
            )
            conn.commit()
            self.stats['inserts'] += insert
            self.stats['updates'] += update
            self.stats['cars'] += len(car_batch)
        
        ThreadSafeLogger.log(
            f"Round {round_num + 1} 완료 - 총 {round_car_count}대 처리", 
            self.thread_id
        )
        
        return True
    
    def run(self):
        """스레드 실행 - 슬라이딩 도어 방식으로 계속 진행"""
        ThreadSafeLogger.log("스레드 시작", self.thread_id)
        
        conn = None
        cursor = None
        
        try:
            # DB 연결 및 드라이버 초기화
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            self.driver_manager.initialize_session()
            
            round_num = 0
            
            # 슬라이딩 도어 방식: 계속해서 다음 라운드 진행
            while True:
                should_continue = self.process_round(round_num, conn, cursor)
                
                if not should_continue:
                    break
                
                self.stats['rounds'] += 1
                round_num += 1
                
                ThreadSafeLogger.log(
                    f"다음 라운드로 이동 - Round {round_num + 1}", 
                    self.thread_id
                )
        
        except Exception as e:
            ThreadSafeLogger.log(f"오류 발생: {e}", self.thread_id)
            traceback.print_exc()
            if conn:
                conn.rollback()
            self.stats['errors'] += 1
        
        finally:
            # 리소스 정리
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            self.driver_manager.quit()
            
            # 통계 큐에 추가
            self.stats_queue.put(self.stats)
            ThreadSafeLogger.log(
                f"스레드 종료 - 총 {self.stats['rounds']}라운드 완료", 
                self.thread_id
            )


class MultiThreadCrawler:
    """멀티스레드 크롤러 메인 클래스 - 슬라이딩 도어 방식"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.stats_queue = Queue()
        self.threads = []
        self.page_allocator = PageAllocator(
            CrawlerConfig.THREAD_COUNT, 
            CrawlerConfig.PAGES_PER_THREAD
        )
    
    def crawl(self):
        """멀티스레드 크롤링 실행 - 슬라이딩 도어 방식"""
        ThreadSafeLogger.log(
            f"멀티스레드 크롤링 시작 - 스레드 수: {CrawlerConfig.THREAD_COUNT}, "
            f"스레드당 페이지: {CrawlerConfig.PAGES_PER_THREAD}"
        )
        ThreadSafeLogger.log("슬라이딩 도어 방식 - 각 스레드가 50페이지씩 처리 후 다음 구간으로 자동 이동")
        
        # 스레드 생성 및 시작
        for i in range(CrawlerConfig.THREAD_COUNT):
            thread_id = i + 1
            
            # 첫 라운드 페이지 범위 출력
            start_page, end_page = self.page_allocator.get_page_range(thread_id, 0)
            
            thread = CrawlerThread(
                thread_id=thread_id,
                page_allocator=self.page_allocator,
                stats_queue=self.stats_queue
            )
            
            self.threads.append(thread)
            thread.start()
            
            ThreadSafeLogger.log(
                f"스레드 {thread_id} 시작 - 첫 번째 페이지 범위: {start_page}~{end_page}"
            )
            
            # 스레드 시작 간격
            time.sleep(2)
        
        # 모든 스레드 완료 대기
        for thread in self.threads:
            thread.join()
        
        # 결과 취합
        self.print_summary()
    
    def print_summary(self):
        """크롤링 결과 요약 출력"""
        end_time = datetime.now()
        elapsed = (end_time - self.start_time).total_seconds() / 60
        
        total_stats = {
            'rounds': 0,
            'pages': 0,
            'cars': 0,
            'inserts': 0,
            'updates': 0,
            'errors': 0
        }
        
        ThreadSafeLogger.log("=" * 80)
        ThreadSafeLogger.log("스레드별 결과:")
        
        while not self.stats_queue.empty():
            stats = self.stats_queue.get()
            ThreadSafeLogger.log(
                f"  스레드 {stats['thread_id']:02d}: "
                f"라운드={stats['rounds']}, 페이지={stats['pages']}, 차량={stats['cars']}, "
                f"INSERT={stats['inserts']}, UPDATE={stats['updates']}, "
                f"오류={stats['errors']}"
            )
            total_stats['rounds'] += stats['rounds']
            total_stats['pages'] += stats['pages']
            total_stats['cars'] += stats['cars']
            total_stats['inserts'] += stats['inserts']
            total_stats['updates'] += stats['updates']
            total_stats['errors'] += stats['errors']
        
        ThreadSafeLogger.log("=" * 80)
        ThreadSafeLogger.log("전체 크롤링 완료")
        ThreadSafeLogger.log(f"소요 시간: {elapsed:.2f}분")
        ThreadSafeLogger.log(f"총 라운드: {total_stats['rounds']}회")
        ThreadSafeLogger.log(f"총 페이지: {total_stats['pages']}개")
        ThreadSafeLogger.log(f"총 차량: {total_stats['cars']}대")
        ThreadSafeLogger.log(f"총 INSERT: {total_stats['inserts']}건")
        ThreadSafeLogger.log(f"총 UPDATE: {total_stats['updates']}건")
        ThreadSafeLogger.log(f"총 오류: {total_stats['errors']}건")
        ThreadSafeLogger.log("=" * 80)


if __name__ == "__main__":
    # 로거 초기화
    ThreadSafeLogger.initialize()
    
    try:
        crawler = MultiThreadCrawler()
        
        # 크롤링 실행 - 슬라이딩 도어 방식
        # Thread 1: 1~50 -> 501~550 -> 1001~1050 -> ...
        # Thread 2: 51~100 -> 551~600 -> 1051~1100 -> ...
        # ...
        # Thread 10: 451~500 -> 951~1000 -> 1451~1500 -> ...
        crawler.crawl()
    
    finally:
        # 로거 종료
        ThreadSafeLogger.close()

