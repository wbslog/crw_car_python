"""
애플리케이션 설정 파일
"""
from __future__ import annotations
import os
from dataclasses import dataclass

CHROME_DRIVER_VER = "141.0.7390.76"

@dataclass
class DatabaseConfig:
    """데이터베이스 설정"""
    HOST: str = os.getenv('DB_HOST', '118.27.108.233')
    PORT: int = int(os.getenv('DB_PORT', 33066))
    USER: str = os.getenv('DB_USER', 'cwdb')
    PASSWORD: str = os.getenv('DB_PASSWORD', 'cwzmfhfflddb')
    DATABASE: str = os.getenv('DB_NAME', 'TS_CW_CARDB')
    CHARSET: str = 'utf8mb4'
    
    # 커넥션 풀 설정
    POOL_SIZE: int = 5
    MAX_OVERFLOW: int = 10
    POOL_RECYCLE: int = 3600  # 1시간


@dataclass
class ProxyConfig:
    """프록시 설정"""
    USER_ID: str = os.getenv('PROXY_USER_ID', 'madone')
    PASSWORD: str = os.getenv('PROXY_PASSWORD', 'trek1024!')



INDEX_SAMPLE_URL = "https://www.kbchachacha.com/public/search/main.kbc"
TARGET_SITE_LP_URL = "https://www.kbchachacha.com/public/search/list.empty?page=VAR_PAGE_NUM&sort=-orderDate"
#CARINFO_SYNC_URL = "http://newcarapi.autoplus.co.kr:10000/carNumberGetData.php?carNumber=CAR_PLATE_NUMBER&vi_no=VI_NUMBER&apiDivide=1"
CARINFO_SYNC_URL = "http://ip.wbslog.com/cp.php?plateNumber=CAR_PLATE_NUMBER"

TARGET_SITE_VIP_URL=""
USER_AGENT_NAME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) chrome=141.0.7390.107 Safari/537.36"
#CHROME_DIRVER_LOC="D:\\chromedriver-win64\\chromedriver.exe"          #Windows version


@dataclass
class CrawlerConfig:
    """크롤러 설정"""
    # 로그인 정보
    LOGIN_URL: str = ""
    LOGIN_ID: str = ""
    LOGIN_PW: str = ""
    LOGIN_NAME: str = ""
    
    # URL 설정
    INDEX_SAMPLE_URL: str = "https://www.kbchachacha.com/public/search/main.kbc"
    CARINFO_SYNC_URL: str = "http://ip.wbslog.com/cp.php?plateNumber=CAR_PLATE_NUMBER"
    MAIN_INDEX_HOST_NAME: str = "https://www.kbchachacha.com/public/search/main.kbc"
    MAIN_HOST_NAME: str = "www.kbchachacha.com"
    
    # 크롬 드라이버 설정
    
    if os.name == 'nt':
        CHROME_DRIVER_FILE: str = os.getenv( "D:\\chromedriver-win64\\143.0.7499.192\\chromedriver.exe")
    else:    
        CHROME_DRIVER_FILE: str = os.getenv("/data/chromedriver-linux64/ver_141.0.7390.78/chromedriver")
    USER_AGENT: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) chrome=141.0.7390.107 Safari/537.36'
    
    # 크롤링 설정
    PAGE_LOAD_TIMEOUT: int = 30
    SCRIPT_TIMEOUT: int = 30
    IMPLICIT_WAIT: int = 20
    MAX_RETRIES: int = 3
    
    # 배치 설정
    BATCH_SIZE: int = 100
    RECONNECT_INTERVAL: int = 15  # 15건마다 재연결
    MIN_SLEEP_SEC: int = 4
    MAX_SLEEP_SEC: int = 6


@dataclass
class SiteConfig:
    """사이트 코드 설정"""
    ENCAR: str = "1000"
    KB_CHACHACHA: str = "2000"


# 싱글톤 인스턴스
db_config = DatabaseConfig()
proxy_config = ProxyConfig()
crawler_config = CrawlerConfig()
site_config = SiteConfig()