"""
Selenium WebDriver 관리 서비스
"""
from __future__ import annotations
import time
import traceback
from typing import Optional
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException



from config.settings import crawler_config
from utils.proxy import ProxyManager
from utils.logger import get_logger

logger = get_logger()


class WebDriverService:
    """WebDriver 관리 서비스"""
    
    def __init__(self):
        self.driver: Optional[webdriver.Chrome] = None
        self.proxy_manager = ProxyManager()
    
    def create_driver(self) -> webdriver.Chrome:
        """
        프록시를 사용한 Chrome WebDriver 생성
        
        Returns:
            Chrome WebDriver 인스턴스
        """
        # 프록시 정보 가져오기
        proxy_info = self.proxy_manager.get_least_used_proxy()
        wire_options = self.proxy_manager.get_proxy_wire_options(proxy_info)
        
        # Chrome 옵션 설정
        chrome_options = self._get_chrome_options()
        
        # Chrome 드라이버 서비스
        service = Service(crawler_config.CHROME_DRIVER_FILE)
        
        # WebDriver 생성
        driver = webdriver.Chrome(
            options=chrome_options,
            service=service,
            seleniumwire_options=wire_options
        )
        
        # 타임아웃 설정
        driver.set_page_load_timeout(crawler_config.PAGE_LOAD_TIMEOUT)
        driver.set_script_timeout(crawler_config.SCRIPT_TIMEOUT)
        driver.implicitly_wait(crawler_config.IMPLICIT_WAIT)
        
        logger.info("WebDriver 생성 완료")
        
        return driver
    
    def _get_chrome_options(self) -> Options:
        """
        Chrome 옵션 생성
        
        Returns:
            Chrome Options 객체
        """
        options = Options()
        
        # 기본 옵션
        options.add_argument(f"--user-agent={crawler_config.USER_AGENT}")
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # SSL/보안 관련 설정
        options.add_argument('--allow-insecure-localhost')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--allow-running-insecure-content')
        
        # 혼합 콘텐츠 허용
        options.add_experimental_option('prefs', {
            'profile.default_content_setting_values': {
                'mixed_script': 1,
                'insecure_content': 1
            }
        })
        
        return options
    
    def initialize(self) -> webdriver.Chrome:
        """
        WebDriver 초기화 및 샘플 페이지 접속
        
        Returns:
            초기화된 WebDriver
        """
        driver = self.create_driver()
        
        try:
            # 샘플 URL 접속
            logger.info(f"샘플 페이지 접속: {crawler_config.INDEX_SAMPLE_URL}")
            driver.get(crawler_config.INDEX_SAMPLE_URL)
            time.sleep(5)
            
            logger.info("WebDriver 초기화 완료")
            
        except Exception as e:
            logger.error(f"WebDriver 초기화 실패: {str(e)}")
            driver.quit()
            raise
        
        self.driver = driver
        return driver
    
    def fetch_page_with_retry(
        self,
        driver: webdriver.Chrome,
        url: str,
        max_retries: int = None
    ) -> Optional[str]:
        """
        재시도 로직이 포함된 페이지 가져오기
        
        Args:
            driver: WebDriver 인스턴스
            url: 크롤링할 URL
            max_retries: 최대 재시도 횟수
            
        Returns:
            페이지 HTML 소스 또는 None
        """
        if max_retries is None:
            max_retries = crawler_config.MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[시도 {attempt + 1}/{max_retries}] URL 접속: {url}")
                
                # 페이지 이동
                driver.get(url)
                time.sleep(2)
                
                # 필수 요소 대기
                wait = WebDriverWait(driver, 30)
                logger.debug("필수 요소 로딩 대기 중...")
                
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'meta[property="og:description"]')
                    )
                )
                
                logger.debug("필수 요소 로딩 완료")
                time.sleep(1)
                
                # HTML 소스 가져오기
                html = driver.page_source
                
                if html and len(html) > 100:
                    logger.info(f"페이지 소스 획득 성공 (크기: {len(html)} bytes)")
                    return html
                else:
                    logger.warning("페이지 소스가 비어있거나 너무 작음")
                    if attempt < max_retries - 1:
                        continue
                
            except TimeoutException:
                logger.warning(f"페이지 로딩 타임아웃 (시도 {attempt + 1}/{max_retries})")
                
                # 페이지 로딩 강제 중단 시도
                try:
                    driver.execute_script("window.stop();")
                    html = driver.page_source
                    
                    if html and len(html) > 100:
                        logger.info("부분 로드된 페이지 사용")
                        return html
                except Exception as stop_error:
                    logger.error(f"페이지 중단 실패: {str(stop_error)}", exc_info=False)
                
                # 재시도 전 대기
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    logger.info(f"{wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                    
                    try:
                        driver.refresh()
                        time.sleep(2)
                    except:
                        pass
            
            except WebDriverException as e:
                error_msg = str(e).lower()
                logger.error(f"WebDriver 오류 (시도 {attempt + 1}/{max_retries}): {str(e)}", exc_info=False)
                
                # 치명적 오류 체크
                if "chrome not reachable" in error_msg or "session deleted" in error_msg:
                    logger.critical("치명적인 드라이버 오류 - 재시도 중단")
                    return None
                
                if attempt < max_retries - 1:
                    time.sleep(3)
            
            except Exception as e:
                logger.error(f"예상치 못한 오류 (시도 {attempt + 1}/{max_retries}): {str(e)}")
                
                if attempt < max_retries - 1:
                    time.sleep(3)
        
        # 모든 재시도 실패
        logger.error(f"최종 실패: {url} - {max_retries}번 시도 모두 실패")
        return None
    
    def close(self):
        """WebDriver 종료"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("WebDriver 종료 완료")
            except:
                pass
            finally:
                self.driver = None