
import os
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
import pymysql
import random
import varList
import requests
import re
from requests.exceptions import HTTPError, Timeout, RequestException
import threading
from queue import Queue
from typing import List
from lxml import html, etree
import traceback
from urllib.parse import urlparse, parse_qs
import sys

# ────────────────────────────────────────────────────────────────────────────────────────────
# 1.URL-Info, DB-Info
DB_USER_ID = varList.dbUserId 
DB_USER_PASS = varList.dbUserPass 
DB_SERVER_HOST = varList.dbServerHost
DB_SERVER_PORT = varList.dbServerPort
DB_SERVER_NAME = varList.dbServerName

PROXY_USER_ID = varList.proxyUserId
PROXY_USER_PASS = varList.proxyUserPass
CHROME_DRIVER_VERSION = varList.CHROME_DRIVER_VER
CHROME_DIRVER_FILE = varList.CHROME_DRIVER_FILE_PATH


INDEX_SAMPLE_URL = "https://carmore.kr/home/?modal=mainPopup"
TARGET_SEARCH_URL = "https://carmore.kr/home/carlist.html?areaCode=VAR_AREA_CODE&rentStartDate=VAR_START_DATE%20VAR_START_TIME&rentEndDate=VAR_END_DATE%20VAR_END_TIME&isOverseas=false&nationalCode=KR&rt=1&locationName=VAR_AREA_NAME&sls=VAR_SLS_CODE"

TARGET_SITE_VIP_URL=""
USER_AGENT_NAME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) chrome=141.0.7390.108 Safari/537.36"
#CHROME_DIRVER_LOC="D:\\chromedriver-win64\\chromedriver.exe"                       #Windows version
CHROME_DIRVER_LOC=""

LOG_SYSTEM_ALIAS="CarMore"
LOG_AREA_CODE=""
LOG_AREA_NAME=""

#OS구분 
#2: windwos, 3: Linux
if os.name == 'nt':
    FILE_SAVE_PATH="D:\\data\\carmore_data\\"            #Windows version
else:
    FILE_SAVE_PATH ="/data/niffler_cm/data/carmore_data/"          #Linux version


START_TIME = datetime.now()

# ────────────────────────────────────────────────────────────────────────────────────────────
# 3. 로그인 정보
LOGIN_URL = ""
LOGIN_ID = ""
LOGIN_PW = ""
LOGIN_NAME=""

# ────────────────────────────────────────────────────────────────────────────────────────────
VAR_PAGE_NUM = 1
VAR_PAGE_BLOCK = 0
VAR_WD_DEFAULT_COUNT = 0
VAR_DB_DEFAULT_COUNT = 0
VAR_INSERT_COUNT = 0
VAR_UPDATE_COUNT = 0

# ────────────────────────────────────────────────────────────────────────────────────────────
def getDbConnectInfo():
    
    return pymysql.connect(
        host=DB_SERVER_HOST,
        port=DB_SERVER_PORT,
        user=DB_USER_ID,
        passwd=DB_USER_PASS,
        db=DB_SERVER_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    ) 
    
#4.# Proxy IP/PORT get
#사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
def getProxyIpOne():    
      
    #MSS DB서버 접속 정보 (varList 파일에서 가져옴)
    conn = pymysql.connect(
        user=DB_USER_ID, 
        passwd=DB_USER_PASS, 
        host=DB_SERVER_HOST, 
        port=DB_SERVER_PORT,
        db=DB_SERVER_NAME,         
        charset='utf8'
    )    

    #사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
    query = '''
                SELECT 
                    PROXY_CODE
                    ,PROXY_IP
                    ,PROXY_PORT
                    ,EXEC_COUNT
                FROM TS_CW_CARDB.NIF_PROXY_LIST 
                WHERE STATUS ='1'
                ORDER BY EXEC_COUNT ASC
                LIMIT 1
            '''

    connInfo = conn.cursor(pymysql.cursors.DictCursor)
    connInfo.execute(query)  #쿼리 실행
    result=connInfo.fetchall()
    df = pd.DataFrame(result)

    proxyInfo = {}
    proxyInfo["PROXY_CODE"]=df['PROXY_CODE'][0]
    proxyInfo["PROXY_IP"]=df['PROXY_IP'][0]
    proxyInfo["PROXY_PORT"]=df['PROXY_PORT'][0]
    proxyInfo["PROXY_USER_ID"]=PROXY_USER_ID
    proxyInfo["PROXY_USER_PASS"]=PROXY_USER_PASS
    
    
    updateQuery = f'''
                UPDATE TS_CW_CARDB.NIF_PROXY_LIST 
                SET  EXEC_COUNT =EXEC_COUNT+1
                WHERE PROXY_CODE = '{proxyInfo["PROXY_CODE"]}'
            '''
    connInfo.execute(updateQuery)

    connInfo.close() ## 연결 정도 자원 반환
    conn.commit() ##물리적으로 데이터 저장을 확정시킴    
    conn.close()

    df.head()
    df.describe()
    
    setLogPrint("get ProxyInfo [IP:"+proxyInfo["PROXY_IP"]+"][PORT:"+proxyInfo["PROXY_PORT"]+"]")
    
    return proxyInfo

# ────────────────────────────────────────────────────────────────────────────────────────────
# General Log Print Function
def setLogPrint(msg):
    global LOG_AREA_CODE
    global LOG_AREA_NAME
    logNowTime = datetime.now()
    logFormatted = logNowTime.strftime("%Y-%m-%d %H:%M:%S")
    print("#[",logFormatted,"][",LOG_AREA_CODE,"#",LOG_AREA_NAME,"][",str(msg),"]")

class WebScraper:
    def __init__(self):
        """
        웹 스크래퍼 초기화
        :param chromedriver_path: ChromeDriver 경로 (None이면 자동 탐색)
        """
        self.driver = None
        self.cookies = None
        self.session = requests.Session()
        self.chromedriver_path = CHROME_DIRVER_LOC        
        
    def setup_driver(self, headless=False):
        
        if self.driver is not None:
            return
        
        """
        Chrome WebDriver 설정
        :param headless: 헤드리스 모드 사용 여부
        """
        
        #proxy ip setting
        proxyInfoMap = getProxyIpOne()
        PROXY_HOST = proxyInfoMap["PROXY_IP"]
        PROXY_PORT = proxyInfoMap["PROXY_PORT"]

        wire_options = {
            'proxy': {
                'http': f'socks5://{PROXY_USER_ID}:{PROXY_USER_PASS}@{PROXY_HOST}:{PROXY_PORT}',
                'https': f'socks5://{PROXY_USER_ID}:{PROXY_USER_PASS}@{PROXY_HOST}:{PROXY_PORT}',
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        
        #chrome_options = Options()
        chrome_options = webdriver.ChromeOptions()        
        if headless:
            chrome_options.add_argument('--headless')
              
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(f'user-agent='+USER_AGENT_NAME) 
        
        # HTTP 강제 설정
        chrome_options.add_argument('--disable-features=AutoupgradeMixedContent')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--allow-insecure-localhost')
        chrome_options.add_argument('--disable-web-security')
                
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        
        service_path = Service(self.chromedriver_path)
        self.driver = webdriver.Chrome(options=chrome_options, service = service_path, seleniumwire_options = wire_options)
        
        # if self.chromedriver_path:
        #     service_path = Service(self.chromedriver_path)
        #      self.driver = webdriver.Chrome(options=chrome_options, service = service_path, seleniumwire_options = wire_options)
        # else:
        #     self.driver = webdriver.Chrome(options=chrome_options, seleniumwire_options = wire_options)
        setLogPrint("ChromeDriverSetup")
        self.driver.implicitly_wait(10)
        
    def connectToObject(self, url, wait_time=3):
        """
        메인 페이지 접속
        :param url: 접속할 URL
        :param wait_time: 페이지 로드 대기 시간 (초)
        """
        setLogPrint("targetUrl"+url)        
        self.driver.get(url)
        time.sleep(wait_time)        
        
    def get_cookies(self):
        """
        현재 브라우저의 쿠키 정보 수집
        :return: 쿠키 딕셔너리
        """
        self.cookies = self.driver.get_cookies()
        print(f"수집된 쿠키 개수: {len(self.cookies)}")
        
        # 쿠키 정보 출력
        # for cookie in self.cookies:
        #     print(f"- {cookie['name']}: {cookie['value'][:50]}...")
        
        return self.cookies
    
    def get_session_storage(self):
        """
        세션 스토리지 정보 수집
        :return: 세션 스토리지 딕셔너리
        """
        session_storage = self.driver.execute_script(
            "return Object.entries(sessionStorage).reduce((acc, [key, value]) => {acc[key] = value; return acc;}, {});"
        )
        print(f"수집된 세션 스토리지 개수: {len(session_storage)}")
        return session_storage
    
    def get_local_storage(self):
        """
        로컬 스토리지 정보 수집
        :return: 로컬 스토리지 딕셔너리
        """
        local_storage = self.driver.execute_script(
            "return Object.entries(localStorage).reduce((acc, [key, value]) => {acc[key] = value; return acc;}, {});"
        )
        print(f"수집된 로컬 스토리지 개수: {len(local_storage)}")
        return local_storage
    
    def create_headers_with_cookies(self, additional_headers=None):
        """
        쿠키 정보를 포함한 헤더 생성
        :param additional_headers: 추가 헤더 딕셔너리
        :return: 헤더 딕셔너리
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        if additional_headers:
            headers.update(additional_headers)
        
        # 쿠키를 세션에 추가
        if self.cookies:
            for cookie in self.cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
        
        return headers
    
    def scrape_more_with_driver(self, url):
        """
        Selenium 드라이버를 사용하여 페이지 수집
        :param url: 수집할 URL
        :return: 페이지 소스
        """
        #print(f"\n드라이버로 페이지 수집 중: {url}")
        self.driver.get(url)        
        
        # 더보기 버튼 계속 클릭
        click_count = 0
        while True:
            try:
                # 더보기 버튼 찾기 (여러 선택자 시도)
                more_button = None
                # selectors = [
                #     "button.more-btn",
                #     "button[class*='more']",
                #     "a.more-btn",
                #     "//button[contains(text(), '더 보기')]",
                #     "//a[contains(text(), '더 보기')]",
                #     ".btn-more",
                #     "#moreBtn"
                # ]
                
                selectors = [                
                    "//button[contains(text(), '더 보기')]"                   
                ]
                
                for selector in selectors:                    
                    try:
                        if selector.startswith("//"):
                            more_button = WebDriverWait(self.driver, 2).until(
                                EC.element_to_be_clickable((By.XPATH, selector))
                            )                         
                        else:
                            more_button = WebDriverWait(self.driver, 2).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                            )                          
                        break
                    except:
                        continue

                if more_button:
                    # 버튼이 보이도록 스크롤                   
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", more_button)
                    time.sleep(0.5)
                    
                    # 클릭
                    more_button.click()
                    click_count += 1
                    print(f"더보기 버튼 클릭: {click_count}회")
                    # 로딩   대기
                    time.sleep(2)   
                    #break;          # 테스트 목적으로 1번만 실행하게 함.  ( 실제 구동시에 break구문 제거 필요)         
                else:
                    print("더보기 버튼을 찾을 수 없음. 종료합니다.")
                    break
                
                #로컬에서 개발시 빠른 결과를 보기 위해 제한함 (실제 배포시 아래 2줄 주석 처리 필요)
                # if(click_count > 2 ):
                #     break
            except TimeoutException:
                print("더 이상 더보기 버튼이 없습니다. 크롤링 시작...")
                break
            except Exception as e:                            
                print("전체 Traceback:")
                traceback.print_exc()
                print(f"더보기 클릭 중 오류: {e}")                
                break

        # 차량 목록 파싱
        time.sleep(2)
        
        page_source = self.driver.page_source
        #print(f"페이지 소스 길이: {len(page_source)} 문자")
        return page_source

    def scrape_with_driver(self, url):
        """
        Selenium 드라이버를 사용하여 페이지 수집
        :param url: 수집할 URL
        :return: 페이지 소스
        """
        #print(f"\n드라이버로 페이지 수집 중: {url}")
        self.driver.get(url)
        
        selectors = [                
                    "//button[contains(text(), '예약하기')]"                   
        ]
        
        for selector in selectors:                    
            try:
                if selector.startswith("//"):
                    more_button = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )                         
                else:
                    more_button = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )                          
                break
            except:
                continue
                        
        page_source = self.driver.page_source
        #print(f"페이지 소스 길이: {len(page_source)} 문자")
        return page_source
    
    def scrape_with_requests(self, url, headers):
        """
        requests 라이브러리를 사용하여 페이지 수집 (쿠키 포함)
        :param url: 수집할 URL
        :param headers: 헤더 딕셔너리
        :return: 응답 객체
        """
        print(f"\nRequests로 페이지 수집 중: {url}")
        response = self.session.get(url, headers=headers)
        print(f"응답 상태 코드: {response.status_code}")
        print(f"응답 길이: {len(response.text)} 문자")
        return response
    
    def login_if_needed(self, username=None, password=None, 
                       username_selector=None, password_selector=None, 
                       submit_selector=None):
        """
        필요시 로그인 수행
        :param username: 사용자명
        :param password: 비밀번호
        :param username_selector: 사용자명 입력 필드 선택자 (CSS Selector)
        :param password_selector: 비밀번호 입력 필드 선택자
        :param submit_selector: 로그인 버튼 선택자
        """
        if not all([username, password, username_selector, password_selector, submit_selector]):
            print("로그인 정보가 완전하지 않습니다. 로그인을 건너뜁니다.")
            return
        
        try:
            print("로그인 시도 중...")
            
            # 사용자명 입력
            username_field = WebDriverWait(self.driver, 7).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, username_selector))
            )
            username_field.clear()
            username_field.send_keys(username)
            
            # 비밀번호 입력
            password_field = self.driver.find_element(By.CSS_SELECTOR, password_selector)
            password_field.clear()
            password_field.send_keys(password)
            
            # 로그인 버튼 클릭
            submit_button = self.driver.find_element(By.CSS_SELECTOR, submit_selector)
            submit_button.click()
            
            time.sleep(3)
            print("로그인 완료")
            
        except Exception as e:
            print("login_if_needed START","="*150)
            print(f"로그인 중 오류 발생: {e}")
            print("login_if_needed END","="*150)
    
    def close(self):
        """
        드라이버 종료
        """
        if self.driver:
            self.driver.quit()
            print("드라이버 종료됨")

def extract_keyword(html, keyword):
    # "자차플러스+" 같이 뒤에 문자가 붙은 경우도 추출
    pattern = f'{keyword}[^<]*'
    match = re.search(pattern, html)
    if match:
        return match.group(0)
    return None

def print_object(obj, title="Object Info"):
    print(f"\n{'='*50}")
    print(f"{title}")
    print('='*50)
    
    obj_dict = {}
    for key, value in obj.__dict__.items():
        # WebDriver 같은 복잡한 객체는 타입만 표시
        if hasattr(value, '__class__') and 'selenium' in str(type(value)):
            obj_dict[key] = f"<{type(value).__name__}>"
        else:
            obj_dict[key] = str(value)
    
    # JSON 형식으로 출력
    print(json.dumps(obj_dict, indent=2, ensure_ascii=False))
    print('='*50 + '\n')
   
#html -> lxml 변환후 정규식으로 데이터 추출
def getValueFromHtml(htmlContent, startTag, endTag, pattern):
    
    START_TIME = datetime.now()
    """lxml로 빠르게 추출"""        
    startIdx = htmlContent.find(startTag)
    if startIdx == -1:
        return None
    else:
        # 시작 태그의 자리수만큼 이전으로 이동
        startIdx = startIdx-(len(startTag)+5)
    
    # 종료 태그 찾기
    endIdx = htmlContent.find(endTag, startIdx)
    if endIdx == -1:
        return None
    else:
        # 끝 태그의 시작위치에서 끝 태그 길이만큼 뒤로 이동
        endIdx = endIdx+(len(endTag)+5)

    print("startIdx:"+str(startIdx)+" | endIdx:"+str(endIdx))
    # 추출
    print("="*50)
    print(htmlContent)
    print("="*50)
    CuttingTag = htmlContent[startIdx:endIdx]
    print("="*50)
    
    setLogPrint("getValueFromHtml>CuttingTag:"+CuttingTag)
    
    # HTML 파싱
    tree = html.fromstring(CuttingTag)
    
    # XPath로 특정 영역 추출 (정규식보다 빠름)
    # 예: <div id="target">...</div> 사이의 내용
    target_elements = tree.xpath('//div[@id="target"]')
    
    if not target_elements:
        return []
    
    # 텍스트 추출
    target_text = etree.tostring(target_elements[0], encoding='unicode', method='html')
        
    # 정규식 적용 (미리 컴파일)
    compiled_pattern = re.compile(pattern)
    results = compiled_pattern.findall(target_text)
    
    END_TIME = datetime.now()
    TIME_GAP = END_TIME - START_TIME
    elapsed_minutes = TIME_GAP.total_seconds()/60
    gapTime = f"{elapsed_minutes:.2f}"
    setLogPrint("Detail Collection Time: "+str(gapTime) +" Sec")    
    
    return results
  

def get_korean_weekday(dateString):
    """날짜 문자열을 받아 한글 요일을 반환"""
    
    # dateString format : 0000-00-00
    # datetime 객체로 변환
    weekdayDict = {
        0: "월요일",
        1: "화요일",
        2: "수요일",
        3: "목요일",
        4: "금요일",
        5: "토요일",
        6: "일요일"
    }
    
    dateObj = datetime.strptime(dateString, "%Y-%m-%d")
    weekdayNum = dateObj.weekday()
    
    return weekdayDict[weekdayNum]

def getAreaName(slsCode):
    """날짜 문자열을 받아 한글 요일을 반환"""
      # -공항 5개
      # 제주국제공항 sls=5 , araCode = 
      # 김포국제공항 sls=7
      # 김해국제공항 sls=2
      # 인천국제공항%201터미널 sls=1
      # 인천국제공항%202터미널 sls=212

      # -KTX역사 6개
      # 서울역 sls=18
      # 영등포역 sls=21
      # 경주역 sls=37
      # 순천역 sls=19
      # 여수엑스포역  sls=20
      # 부산역 sls=26       
        
        
    # dateString format : 0000-00-00
    # datetime 객체로 변환
    areaNameDic = {
        '5':   "제주국제공항",
        '7':   "김포국제공항",
        '2':   "김해국제공항",
        '1':   "인천국제공항%201터미널",
        '212': "인천국제공항%202터미널",
        '18':  "서울역",
        '21':  "영등포역",
        '37':  "경주역",
        '19':  "순천역",
        '20':  "여수엑스포역",
        '26':  "부산역"
    }    
    return areaNameDic[slsCode]

def getAreaCode(slsCode):
    """날짜 문자열을 받아 한글 요일을 반환"""
      # -공항 5개
      # 제주국제공항 sls=5 , araCode = Q_1
      # 김포국제공항 sls=7 , areaCode = A_16
      # 김해국제공항 sls=2 , areaCode = M_12
      # 인천국제공항%201터미널 sls=1 , areaCode = B_1
      # 인천국제공항%202터미널 sls=212 , areaCode = B_1

      # -KTX역사 6개
      # 서울역 sls=18 , areaCode = A_2
      # 영등포역 sls=21 , areaCode = A_19
      # 경주역 sls=37 , areaCode = O_3
      # 순천역 sls=19 , areaCode = I_3
      # 여수엑스포역 sls=20 , areaCode = I_2
      # 부산역 sls=26 , areaCode = M_3
        
        
    # dateString format : 0000-00-00
    # datetime 객체로 변환
    areaCodeDic = {
        '5':   "Q_1",
        '7':   "A_16",
        '2':   "M_12",
        '1':   "B_1",
        '212': "B_1",
        '18':  "A_2",
        '21':  "A_19",
        '37':  "O_3",
        '19':  "I_3",
        '20':  "I_2",
        '26':  "M_3"
    }    
    return areaCodeDic[slsCode]

def getCatYearsName(text):
    
    if not text:
        return None
    
    # 케이스 1: "-" 또는 "~"가 포함된 경우 (예: "24~25년식", "24-25년식")
    if '-' in text or '~' in text:
        # 마지막 숫자 2자리 추출
        matches = re.findall(r'\d{2}', text)
        if matches:
            year_2digit = matches[-1]  # 마지막 2자리 숫자
            year_4digit = convert_to_4digit_year(year_2digit)
            return year_4digit
    
    # 케이스 2: 숫자가 2자리만 포함된 경우 (예: "25년식")
    matches = re.findall(r'\d+', text)
    if matches and len(matches) == 1 and len(matches[0]) == 2:
        year_2digit = matches[0]
        year_4digit = convert_to_4digit_year(year_2digit)
        return year_4digit
    
    # 케이스 3: 이미 4자리 연도인 경우 (예: "2025년식")
    matches = re.findall(r'\d{4}', text)
    if matches:
        return int(matches[0])
    
    return None
      
def convert_to_4digit_year(year_2digit):
    """
    2자리 연도를 4자리 연도로 변환
    
    Args:
        year_2digit: 2자리 연도 문자열 (예: "25", "98")
    
    Returns:
        int: 4자리 연도 (예: 2025, 1998)
    """
    year = int(year_2digit)
    
    # 현재 연도 기준으로 판단 (예: 2024년 기준)
    # 50 이상이면 1900년대, 50 미만이면 2000년대로 가정
    if year >= 50:
        return 1900 + year
    else:
        return 2000 + year
  
def setBatchExecLog(logDivide, batchUUID, slsCode, areaCode, execCount,totalCount, execText):
   
   
    try :
        BatchDbConn = getDbConnectInfo()       
        BatchDbCursor = BatchDbConn.cursor()
        
        if logDivide == "START":            
            insertQuery = f'''
                INSERT INTO LOG_BATCH_EXEC_LOG(
                    LOG_DIVIDE
                    ,BATCH_UUID
                    ,AREA_CODE
                    ,SLS_CODE
                    ,START_DATE                  
                    ,EXEC_COUNT
                    ,TOTAL_COUNT
                    ,EXEC_TEXT
                    ,ADD_DATE
                ) VALUES (
                    '{logDivide}'
                    ,'{batchUUID}'
                    ,'{areaCode}'
                    ,'{slsCode}'
                    ,NOW()            
                    ,'{execCount}' 
                    ,'{totalCount}' 
                    ,'{execText}' 
                    ,NOW()           
                )                    
            '''
        elif logDivide == "ING" :
            insertQuery = f'''
                INSERT INTO LOG_BATCH_EXEC_LOG(
                    LOG_DIVIDE
                    ,BATCH_UUID
                    ,AREA_CODE   
                    ,SLS_CODE             
                    ,EXEC_COUNT
                    ,TOTAL_COUNT
                    ,EXEC_TEXT
                    ,ADD_DATE
                ) VALUES (
                    '{logDivide}'
                    ,'{batchUUID}'
                    ,'{areaCode}'                   
                    ,'{slsCode}'
                    ,'{execCount}' 
                    ,'{totalCount}' 
                    ,'{execText}' 
                    ,NOW()           
                )                    
            '''
        else:
            insertQuery = f'''
                INSERT INTO LOG_BATCH_EXEC_LOG(
                    LOG_DIVIDE
                    ,BATCH_UUID
                    ,AREA_CODE     
                    ,SLS_CODE    
                    ,END_DATE
                    ,EXEC_COUNT
                    ,TOTAL_COUNT
                    ,EXEC_TEXT
                    ,ADD_DATE
                ) VALUES (
                    '{logDivide}'
                    ,'{batchUUID}'
                    ,'{areaCode}'
                    ,'{slsCode}'
                    ,NOW()
                    ,'{execCount}' 
                    ,'{totalCount}' 
                    ,'{execText}' 
                    ,NOW()           
                )                    
            '''
            
        BatchDbCursor.execute(insertQuery) 
            
        BatchDbCursor.close()        
        BatchDbConn.commit()
        BatchDbConn.close()     
    except Exception as e:
        setLogPrint(f"오류 발생: {e}")        
        setLogPrint("전체 Traceback:")
        traceback.print_exc()
        raise


def setDbUpdateRentCarSaleStatusclose(dbConn, cursor, startYmd, areaCode, areaName, slsCode):

    updateCloseQuery = f'''
        UPDATE TBL_RENTCAR_PRICE_COMPARE_LIST
        SET SALE_STATUS ='3'
        WHERE SALE_STATUS ='1'
        AND START_YMD = '{startYmd}'
        AND AREA_CODE = '{areaCode}'
        AND SLS_CODE = '{slsCode}'
        AND STATUS ='1'
    '''
    cursor.execute(updateCloseQuery)
    dbConn.commit()
    setLogPrint("INFO[SALE_STATUS]: ++ UPDATE > CLOSE slsCode:"+slsCode+" | areaCode:"+areaCode+" | areaName:"+areaName+" | startYmd:"+startYmd )
    print("close "*25)

def setDbInsertRentCarHistInfo(dbConn, cursor, carInfo, beforeMemPrice):
    
    global VAR_START_DATE
    global VAR_START_TIME
    global VAR_END_DATE
    global VAR_END_TIME
    
    insertQueryHist = ""
    
    # Hist Insert
    insertQueryHist = f'''            
    INSERT INTO TBL_RENTCAR_PRICE_COMPARE_HIST (
        SITE_CODE
        ,AREA_NAME
        ,AREA_CODE
        ,SLS_CODE
        ,COMPANY_NAME
        ,PRODUCT_ID
        ,DOMESTIC               -- 국산/수입 구분(1:국산,3:수입)
        ,KIND
        ,MAKER
        ,MODEL
        ,MODEL_DETAIL
        ,GRADE
        ,GRADE_DETAIL
        ,FULL_NAME
        ,COLOR
        ,MISSION
        ,FUEL
        ,YEARS
        ,MODEL_ID
        ,MODEL_CODE1
        ,MODEL_CODE2
        ,MODEL_CODE3
        ,MODEL_CODE4
        ,WEEK_DIVIDE            -- 요일명 입력됨
        ,INSURANCE_DIVIDE      -- 보험구분(일반자차, 완전자차, 슈퍼자차)
        ,MEM_RENT_PRICE
        ,MEM_BEFORE_PRICE
        ,DETAIL_URL
        ,LIST_URL
        ,ADD_DATE
        ,ADD_YMD      
        ,STATUS
        ,SYNC_STATUS            -- 배치연동 상태(1: 대기, 3: 완료)
        ,SYNC_TEXT
        ,START_YMD
        ,START_TIME
        ,END_YMD
        ,END_TIME
        ,SIS_CODE
        ,VER_CODE
        ,RCS_CODE
        ,SELF_PLUS
        ,SALE_STATUS
    )VALUES(		
        '{carInfo["siteCode"]}'
        ,'{carInfo["areaName"]}'                
        ,'{carInfo["areaCode"]}'
        ,'{carInfo["slsCode"]}'
        ,'{carInfo["companyName"]}'
        ,''
        ,''
        ,''
        ,''
        ,'{carInfo["model"]}'
        ,'{carInfo["modelDetail"]}'
        ,'{carInfo["grade"]}'
        ,'{carInfo["gradeDetail"]}'
        ,'{carInfo["fullName"]}'
        ,''
        ,'{carInfo["mission"]}'
        ,'{carInfo["carFuel"]}'
        ,'{carInfo["carYears"]}'
        ,''
        ,''
        ,''
        ,''
        ,''
        ,'{carInfo["weekName"]}'
        ,'{carInfo["insuranceDivide"]}' 
        ,'{carInfo["memRentPrice"]}'
        ,'{beforeMemPrice}'        
        ,'{carInfo["detailUrl"]}'
        ,'{carInfo["listPageUrl"]}'
        ,NOW()
        ,DATE_FORMAT(NOW(), '%Y-%m-%d')
        ,'1'
        ,'3'
        ,'' 
        ,'{VAR_START_DATE}'
        ,'{VAR_START_TIME}'
        ,'{VAR_END_DATE}'
        ,'{VAR_END_TIME}'
        ,'{carInfo["sisCode"]}'
        ,'{carInfo["verCode"]}'
        ,'{carInfo["rcsCode"]}'
        ,'{carInfo["selfPlus"]}'
        ,'1'
    )
    '''
    cursor.execute(insertQueryHist)     
    dbConn.commit()       
    setLogPrint("INFO[HIST]: ++ INSERT > companyName:"+carInfo["companyName"]+" | fullName:"+carInfo["fullName"]+" | startYmd:"+carInfo["startYmd"]+" | newPrice:"+carInfo["memRentPrice"] )
    setLogPrint("INFO[HIST]: >>>>>>>>>>> fullName: "+carInfo["fullName"]+"years:"+str(carInfo["carYears"])+" | fuel:"+carInfo["carFuel"]+" | mission: "+carInfo["mission"]+" | insuranceDivide:"+carInfo["insuranceDivide"]+" | selfPlus:"+carInfo["selfPlus"] )
    print("-"*150)

def setDbInsertRentcarInfo(dbConn, cursor, carInfo):
    
    global VAR_START_DATE
    global VAR_START_TIME
    global VAR_END_DATE
    global VAR_END_TIME
    
    insertQuery = ""
    updateQuery = ""   
     
    try:
        query = f''' 
            SELECT 
                MEM_RENT_PRICE
            FROM TBL_RENTCAR_PRICE_COMPARE_LIST 
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND AREA_CODE = '{carInfo["areaCode"]}'
            AND SLS_CODE = '{carInfo["slsCode"]}'
            AND SIS_CODE = '{carInfo["sisCode"]}'
            AND RCS_CODE = '{carInfo["rcsCode"]}'            
            AND START_YMD = '{carInfo["startYmd"]}'
            AND FULL_NAME = '{carInfo["fullName"]}'
            AND MISSION = '{carInfo["mission"]}'
            AND FUEL = '{carInfo["carFuel"]}'
            AND YEARS = '{carInfo["carYears"]}'
            AND INSURANCE_DIVIDE = '{carInfo["insuranceDivide"]}'
        '''
        #AND ADD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')  제거함
        #print("###query::",query)
        cursor.execute(query)
        result1 = cursor.fetchall()
        dbData = pd.DataFrame(result1)
        
        if not result1 :
            # New Insert
            insertQuery = f'''            
            INSERT INTO TBL_RENTCAR_PRICE_COMPARE_LIST (
                SITE_CODE
                ,AREA_NAME
                ,AREA_CODE
                ,SLS_CODE
                ,COMPANY_NAME
                ,PRODUCT_ID
                ,DOMESTIC               -- 국산/수입 구분(1:국산,3:수입)
                ,KIND
                ,MAKER
                ,MODEL
                ,MODEL_DETAIL
                ,GRADE
                ,GRADE_DETAIL
                ,FULL_NAME
                ,COLOR
                ,MISSION
                ,FUEL
                ,YEARS
                ,MODEL_ID
                ,MODEL_CODE1
                ,MODEL_CODE2
                ,MODEL_CODE3
                ,MODEL_CODE4
                ,WEEK_DIVIDE            -- 요일명 입력됨
                ,INSURANCE_DIVIDE      -- 보험구분(일반자차, 완전자차, 슈퍼자차)
                ,MEM_RENT_PRICE
                ,MEM_BEFORE_PRICE
                ,DETAIL_URL
                ,LIST_URL
                ,ADD_DATE
                ,ADD_YMD      
                ,STATUS
                ,SYNC_STATUS            -- 배치연동 상태(1: 대기, 3: 완료)
                ,SYNC_TEXT
                ,START_YMD
                ,START_TIME
                ,END_YMD
                ,END_TIME
                ,SIS_CODE
                ,VER_CODE
                ,RCS_CODE
                ,SELF_PLUS
                ,SALE_STATUS
            )VALUES(		
                '{carInfo["siteCode"]}'
                ,'{carInfo["areaName"]}'                
                ,'{carInfo["areaCode"]}'
                ,'{carInfo["slsCode"]}'
                ,'{carInfo["companyName"]}'
                ,''
                ,''
                ,''
                ,''
                ,'{carInfo["model"]}'
                ,'{carInfo["modelDetail"]}'
                ,'{carInfo["grade"]}'
                ,'{carInfo["gradeDetail"]}'
                ,'{carInfo["fullName"]}'
                ,''
                ,'{carInfo["mission"]}'
                ,'{carInfo["carFuel"]}'
                ,'{carInfo["carYears"]}'
                ,''
                ,''
                ,''
                ,''
                ,''
                ,'{carInfo["weekName"]}'
                ,'{carInfo["insuranceDivide"]}' 
                ,'{carInfo["memRentPrice"]}'
                ,'0'        
                ,'{carInfo["detailUrl"]}'
                ,'{carInfo["listPageUrl"]}'
                ,NOW()
                ,DATE_FORMAT(NOW(), '%Y-%m-%d')
                ,'1'
                ,'3'
                ,'' 
                ,'{VAR_START_DATE}'
                ,'{VAR_START_TIME}'
                ,'{VAR_END_DATE}'
                ,'{VAR_END_TIME}'
                ,'{carInfo["sisCode"]}'
                ,'{carInfo["verCode"]}'
                ,'{carInfo["rcsCode"]}'
                ,'{carInfo["selfPlus"]}'
                ,'1'
            )
            '''
            cursor.execute(insertQuery)     
            dbConn.commit()                
            setLogPrint("INFO[NEW]: ++ INSERT > companyName:"+carInfo["companyName"]+" | fullName:"+carInfo["fullName"]+" | startYmd:"+carInfo["startYmd"]+" | memPrice:"+carInfo["memRentPrice"] )
            setLogPrint("INFO[NEW]: >>>>>>>>>>> fullName: "+carInfo["fullName"]+"years:"+str(carInfo["carYears"])+" | fuel:"+carInfo["carFuel"]+" | mission: "+carInfo["mission"]+" | insuranceDivide:"+carInfo["insuranceDivide"]+" | selfPlus:"+carInfo["selfPlus"] )
            
            # Hist Insert
            setDbInsertRentCarHistInfo(dbConn, cursor, carInfo, 0 )
            print("-"*150)            
        else:
            if dbData["MEM_RENT_PRICE"][0] == carInfo["memRentPrice"]:
                # 기존 금액과 같다면, SALE_STATUS 값 1로 업데이트 (판매중)                 
                updateQuery = f'''
                    UPDATE TBL_RENTCAR_PRICE_COMPARE_LIST
                    SET	
                        SALE_STATUS ='1'
                        ,MEM_RENT_PRICE =  '{carInfo["memRentPrice"]}'
                        ,COMPANY_NAME =  '{carInfo["companyName"]}'
                        ,MEM_BEFORE_PRICE = '0' 
                        ,SELF_PLUS= '{carInfo["selfPlus"]}'
                        ,LIST_URL='{carInfo["listPageUrl"]}'
                        ,MOD_DATE = NOW()
                        ,MOD_YMD = DATE_FORMAT(NOW(), '%Y-%m-%d')
                        ,MOD_DATE = NOW()
                        ,MOD_YMD = DATE_FORMAT(NOW(), '%Y-%m-%d')                        
                    WHERE STATUS = '1'
                    AND SITE_CODE = '{carInfo["siteCode"]}'
                    AND AREA_CODE = '{carInfo["areaCode"]}'
                    AND SLS_CODE = '{carInfo["slsCode"]}'
                    AND SIS_CODE = '{carInfo["sisCode"]}'
                    AND RCS_CODE = '{carInfo["rcsCode"]}'            
                    AND START_YMD = '{carInfo["startYmd"]}'
                    AND FULL_NAME = '{carInfo["fullName"]}'
                    AND MISSION = '{carInfo["mission"]}'
                    AND FUEL = '{carInfo["carFuel"]}'
                    AND YEARS = '{carInfo["carYears"]}'
                    AND INSURANCE_DIVIDE = '{carInfo["insuranceDivide"]}'               
                '''            
                cursor.execute(updateQuery)   
                dbConn.commit()              
                setLogPrint("INFO[SALE_STATUS]: -- UPDATE > companyName:"+carInfo["companyName"]+" | fullName:"+carInfo["fullName"]+" | startYmd:"+carInfo["startYmd"]+" | memPrice:"+str(carInfo["memRentPrice"]) )
                setLogPrint("INFO[SALE_STATUS] >>>>>>>>>>> fullName: "+carInfo["fullName"]+"years:"+str(carInfo["carYears"])+" | fuel:"+carInfo["carFuel"]+" | mission: "+carInfo["mission"]+" | insuranceDivide:"+carInfo["insuranceDivide"]+" | selfPlus:"+carInfo["selfPlus"] )
                print("-"*150)
            else :
                # 기존 금액과 다르다면, SALE_STATUS 값 1로 업데이트 및 가격 업데이트 -> Hist 테이블에 Insert포함
                updateQuery = f'''
                    UPDATE TBL_RENTCAR_PRICE_COMPARE_LIST    
                    SET	
                        SALE_STATUS ='1'
                        ,MEM_RENT_PRICE =  '{carInfo["memRentPrice"]}'
                        ,COMPANY_NAME =  '{carInfo["companyName"]}'
                        ,MEM_BEFORE_PRICE = MEM_RENT_PRICE
                        ,SELF_PLUS= '{carInfo["selfPlus"]}'
                        ,LIST_URL='{carInfo["listPageUrl"]}'
                        ,MOD_DATE = NOW()
                        ,MOD_YMD = DATE_FORMAT(NOW(), '%Y-%m-%d')                        
                    WHERE STATUS = '1'
                    AND SITE_CODE = '{carInfo["siteCode"]}'
                    AND AREA_CODE = '{carInfo["areaCode"]}'
                    AND SLS_CODE = '{carInfo["slsCode"]}'
                    AND SIS_CODE = '{carInfo["sisCode"]}'
                    AND RCS_CODE = '{carInfo["rcsCode"]}'            
                    AND START_YMD = '{carInfo["startYmd"]}'
                    AND FULL_NAME = '{carInfo["fullName"]}'
                    AND MISSION = '{carInfo["mission"]}'
                    AND FUEL = '{carInfo["carFuel"]}'
                    AND YEARS = '{carInfo["carYears"]}'
                    AND INSURANCE_DIVIDE = '{carInfo["insuranceDivide"]}'            
                '''            
                cursor.execute(updateQuery)   
                dbConn.commit()                
                setLogPrint("INFO[PRICE_NOT_SAME]: -- UPDATE > companyName:"+carInfo["companyName"]+" | fullName:"+carInfo["fullName"]+" | startYmd:"+carInfo["startYmd"]+" | memPrice:"+str(carInfo["memRentPrice"]) )
                setLogPrint("INFO[PRICE_NOT_SAME]: >>>>>>>>>>> fullName: "+carInfo["fullName"]+"years:"+str(carInfo["carYears"])+" | fuel:"+carInfo["carFuel"]+" | mission: "+carInfo["mission"]+" | insuranceDivide:"+carInfo["insuranceDivide"]+" | selfPlus:"+carInfo["selfPlus"] )
                # Hist Insert
                setDbInsertRentCarHistInfo(dbConn, cursor, carInfo, dbData["MEM_RENT_PRICE"][0])        
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))       
        traceback.print_exc()   
        setLogPrint("error:insertQuery:"+str(insertQuery))
        setLogPrint("error:updateQuery:"+str(updateQuery))
        pass
    
def getType3LpDataListInsert(dbConn, dbCursor, slsCode, soup, startDate, listPageUrl):
    # ID로 요소 찾기
    # CATE > LP > VIP
    rentPriceInfo = {}
    areaName = getAreaName(slsCode)
    areaCode = getAreaCode(slsCode)
    rentPriceInfo["areaName"]  = areaName
    rentPriceInfo["areaCode"]  = areaCode
    rentPriceInfo["slsCode"]  = slsCode
    rentPriceInfo["siteCode"] = "1"
    rentPriceInfo["startYmd"] = startDate
    rentPriceInfo["weekName"] = get_korean_weekday(startDate)
    rentPriceInfo["carYears"] = ""
    rentPriceInfo["carFuel"] = ""
        
    fullName = soup.select_one('span[id^="qa_car_list_model_name"]').get_text()
    modelNameArray = fullName.split()
    rentPriceInfo["fullName"]       = fullName
    rentPriceInfo["model"]          = modelNameArray[0] if len(modelNameArray) > 0 else ""
    rentPriceInfo["modelDetail"]    = modelNameArray[1] if len(modelNameArray) > 1 else ""
    rentPriceInfo["grade"]          = modelNameArray[2] if len(modelNameArray) > 2 else ""
    rentPriceInfo["gradeDetail"]    = modelNameArray[3] if len(modelNameArray) > 3 else ""    
    rentPriceInfo["companyName"] =soup.select_one('span[id^="qa_car_list_company_name"]').get_text()
    gear_div = soup.find('img', alt='자동기어 여부')
    if gear_div:
        mission = gear_div.find_next('span').get_text()
        rentPriceInfo["mission"] = mission
    else:
        rentPriceInfo["mission"] =""
    
    rentCompanyArray = soup.find_all('div', class_=lambda x: x and x.startswith('style_Layout__uln9_'))
    for idx1, rentCompanyArrayData in enumerate(rentCompanyArray, 1):
        rentPriceInfo["companyName"] =rentCompanyArrayData.select_one('span[id^="qa_car_list_company_name"]').get_text()
        
       
        if '자차플러스' in str(rentCompanyArrayData):
            rentPriceInfo["selfPlus"] = "자차플러스"
        else:
            rentPriceInfo["selfPlus"] = ""
              
        rentPriceArray = rentCompanyArrayData.find_all('a', id=lambda x: x and x.startswith('qa_car_list_discount_price'))
        for idx1, rentPriceData in enumerate(rentPriceArray, 1):
            rentPriceInfo["memRentPrice"] = rentPriceData.select_one('strong[class^="ColorRedDark "]').text.replace(",","").replace("원","")
            rentPriceInfo["insuranceDivide"]="일반자차"
            mainOptions = rentPriceData.find('div', class_=lambda x: x and x.startswith('style_MainOptions'))
            
            if mainOptions:        
                yearTag = mainOptions.find('b', string=lambda t: t and '년식' in t)
                yearText = yearTag.get_text(strip=True)
                yearRegexp = re.search(r'\d{4}', yearText)
                if yearRegexp:
                    rentPriceInfo["carYears"] = yearRegexp.group()       
                
                dotSpan = soup.find('span', string=lambda t: t and 'ㆍ' in t)

                if dotSpan:
                    fuelTag = dotSpan.find_next('b')
                    if fuelTag:
                        rentPriceInfo["carFuel"] = fuelTag.get_text(strip=True)            
            
            rentPriceInfo["detailUrl"] = rentPriceData.get('href')
            rentPriceInfo["listPageUrl"]  = listPageUrl
            urlParse = urlparse(rentPriceInfo["detailUrl"])
            paramsList = parse_qs(urlParse.query)        
            rentPriceInfo["rcsCode"]=paramsList['rcs'][0]
            rentPriceInfo["sisCode"]=paramsList['sis'][0]
            rentPriceInfo["verCode"]=paramsList['v'][0]         
            setDbInsertRentcarInfo(dbConn, dbCursor, rentPriceInfo)
    
def getType2LpDataListInsert(dbConn, dbCursor, slsCode, soup, startDate, listPageUrl):
    # ID로 요소 찾기
    # LP > VIP
    rentPriceInfo = {}
    areaName = getAreaName(slsCode)
    areaCode = getAreaCode(slsCode)
    rentPriceInfo["areaName"]  = areaName
    rentPriceInfo["areaCode"]  = areaCode
    rentPriceInfo["slsCode"]  = slsCode
    rentPriceInfo["siteCode"] = "1"
    rentPriceInfo["startYmd"] = startDate
    rentPriceInfo["weekName"] = get_korean_weekday(startDate)
    
    fullName = soup.select_one('div[class^="style_CarModel_"]').get_text()
    modelNameArray = fullName.split()
    rentPriceInfo["fullName"]       = fullName
    rentPriceInfo["model"]          = modelNameArray[0] if len(modelNameArray) > 0 else ""
    rentPriceInfo["modelDetail"]    = modelNameArray[1] if len(modelNameArray) > 1 else ""
    rentPriceInfo["grade"]          = modelNameArray[2] if len(modelNameArray) > 2 else ""
    rentPriceInfo["gradeDetail"]    = modelNameArray[3] if len(modelNameArray) > 3 else ""
    
    rentPriceInfo["mission"] = soup.select_one('div[class^="style_Transmission"] div').text
    
    
    companyArray = soup.find_all('div', class_=lambda x: x and x.startswith('style_Layout__J8EEc')) 
    for idx1, companyArrayList in enumerate(companyArray, 1):
        rentPriceInfo["companyName"] = companyArrayList.select_one('span[id^="qa_car_list_company_name_"]').text
        companyArrayListCards = companyArrayList.find_all('div', class_=lambda x: x and x.startswith('style_InsuranceCard_')) 
        for idx2, companyArrayListCard in enumerate(companyArrayListCards, 1):       
            rentPriceInfo["insuranceDivide"] = companyArrayListCard.select_one('div[class^="style_InsuranceDefaultInfo"] span:first-child').text
            rentPriceInfo["carYears"] = getCatYearsName(companyArrayListCard.select_one('div[class^="style_YearNFuel"] span:nth-child(1)').text)
            rentPriceInfo["carFuel"] = companyArrayListCard.select_one('div[class^="style_YearNFuel"] span:nth-child(2)').text.replace("ㆍ","")
            rentPriceInfo["memRentPrice"] = companyArrayListCard.select_one('div[class^="style_SecondRow_"] span:nth-child(3)').text.replace("원","").replace(",","")
            rentPriceInfo["detailUrl"] = companyArrayListCard.find('a', id=lambda x: x and x.startswith('qa_car_list_discount_price')).get('href')         
            if '자차플러스' in str(companyArrayListCard):
                rentPriceInfo["selfPlus"] = "자차플러스"
            else:
                rentPriceInfo["selfPlus"] = ""
            
            rentPriceInfo["listPageUrl"]  = listPageUrl
            urlParse = urlparse(rentPriceInfo["detailUrl"])
            paramsList = parse_qs(urlParse.query)
            
            if paramsList['rcs'][0].count('#') >= 2:
                rcsTmp = re.search(r'#([^#]*)#', paramsList['rcs'][0])
                if rcsTmp:
                    rentPriceInfo["rcsCode"] = rcsTmp.group(1)  
            else:
                rentPriceInfo["rcsCode"]= paramsList['rcs'][0] 
                
            if paramsList['sis'][0].count('#') >= 2:
                sisTmp = re.search(r'#([^#]*)#', paramsList['sis'][0])
                if sisTmp:
                    rentPriceInfo["sisCode"] = sisTmp.group(1)  
            else:
                rentPriceInfo["sisCode"]= paramsList['sis'][0]            
        
            rentPriceInfo["verCode"]=paramsList['v'][0]              
            setDbInsertRentcarInfo(dbConn, dbCursor, rentPriceInfo)
    

# 2 Depth Type (일반 공항/ktx역 대상)
def getListPageToCompanyList(dbConn, dbCursor, slsCode, collector, companyData, companyCount, startDate, listPageUrl):

    procNum = 0
    for idx, companyList in enumerate(companyData, 1):
      
        areaName = getAreaName(slsCode)
        areaCode = getAreaCode(slsCode) 
        
        procNum +=1
        setLogPrint(f"ListPage >> areaName(slsCode): {areaName}({slsCode})  |  proc/total: {procNum}/{companyCount}")        
        getType3LpDataListInsert(dbConn, dbCursor, slsCode, companyList, startDate, listPageUrl)   

# 3 Depth Type (제주국제공항 대상)
def getCatalogToCompanyList(dbConn, dbCursor, slsCode, collector, catalogData, catalogCount, startDate):
          
    catalogDataCount = len(catalogData)
    procNum = 0
    for idx, link in enumerate(catalogData, 1):
        hrefLink = link.get('href')  
        
        areaName = getAreaName(slsCode)
        areaCode = getAreaCode(slsCode) 
                 
        #setLogPrint("##### VIP-URL:"+detailLink)
        htmlContent = collector.scrape_with_driver(hrefLink)    
        
        startIdx = htmlContent.find('style_SearchControllerLayout')  
        endIdx  = htmlContent.find('style_Channels')
        lphtml = htmlContent[startIdx:endIdx]
        soup = BeautifulSoup(lphtml, 'html.parser')      
        
        procNum +=1
        setLogPrint(f"Catalog >> areaName(slsCode): {areaName}({slsCode})  |  proc/total: {procNum}/{catalogDataCount}")        
        getType2LpDataListInsert(dbConn, dbCursor, slsCode, soup, startDate, hrefLink)        
   
def mainProcess(startDate,startTime, endDate, endTime, slsCode, areaCode, areaName):
   
    #DB Connect Info
    dbConn = getDbConnectInfo()
    dbCursor = dbConn.cursor() 
    
    VAR_DB_DEFAULT_COUNT = 0
    
    # 컬렉터 인스턴스 생성 ( class )
    collector = WebScraper()
    
    try:
        
        # List Count
        listTotalCount = 0
        
        # List Type ( 2: 2depth, 3: 3depth )
        listType = 0
        
        # 1. 드라이버 설정
        collector.setup_driver(headless=True)
        
        # 2. 메인 페이지 접속
        indexUrl = INDEX_SAMPLE_URL            
        collector.connectToObject(indexUrl)      
        
        # 4. 쿠키 및 세션, 로컬 스토리지 수집
        cookies = collector.get_cookies()
        session_storage = collector.get_session_storage()
        local_storage = collector.get_local_storage()
        
        # 5. 헤더 생성
        headers = collector.create_headers_with_cookies()
        
        TARGET_SITE_URL=TARGET_SEARCH_URL.replace("VAR_START_DATE",startDate)
        TARGET_SITE_URL=TARGET_SITE_URL.replace("VAR_START_TIME",startTime)
        TARGET_SITE_URL=TARGET_SITE_URL.replace("VAR_END_DATE",endDate)
        TARGET_SITE_URL=TARGET_SITE_URL.replace("VAR_END_TIME",endTime)        
        TARGET_SITE_URL=TARGET_SITE_URL.replace("VAR_AREA_NAME",areaName)
        TARGET_SITE_URL=TARGET_SITE_URL.replace("VAR_AREA_CODE",areaCode)
        TARGET_SITE_URL=TARGET_SITE_URL.replace("VAR_SLS_CODE",slsCode)
       
        #-----------------------------------------------------------------------------------
        # 더보기 버튼 클릭 포함
        setLogPrint("# LP-URL:"+TARGET_SITE_URL)
        mainSource = collector.scrape_more_with_driver(TARGET_SITE_URL)  
        
        # -------------- hmle  -> soup -> 변환 및 추출      
        htmlTag = BeautifulSoup(mainSource, 'html.parser')
        catalogData = htmlTag.find_all('a', class_=lambda x: x and x.startswith('style_CompareAllPricesButton')) 
        
        listTotalCount = 0
        pageTotalCount = 0
        pageSeq = 0
        
        #sale_status = 3 udpate proc
        setDbUpdateRentCarSaleStatusclose(dbConn, dbCursor, startDate, areaCode, areaName, slsCode)
                    
        if len(catalogData) > 0 :
             #(3 Depth) CATALOG > LP > VIP 방식   
            pageSeq+=1
            catalogCount = len(catalogData)             
            listType = 3            
            setLogPrint(f"# LIST TYPE: 2 (LP>VIP) | COUNT:{catalogCount}")                        
            getCatalogToCompanyList(dbConn, dbCursor, slsCode, collector, catalogData, catalogCount, startDate)             
        else:      
             #(2 Depth) LP > VIP 방식 
            pageSeq+=1
            companyLists = htmlTag.find_all('div', class_=lambda x: x and x.startswith('car-model-card')) 
            companyCount = len(companyLists)
            listType = 2            
            setLogPrint(f"# LIST TYPE: 3 (CT>LP>VIP) | COUNT:{companyCount}")  
            getListPageToCompanyList(dbConn, dbCursor, slsCode, collector, companyLists, companyCount, startDate, TARGET_SITE_URL)
        VAR_PAGE_BLOCK = 0
            
            
        if VAR_PAGE_BLOCK > 10:
            VAR_PAGE_BLOCK = 0
            VAR_DB_DEFAULT_COUNT+=1
                        
            dbCursor.close()        
            dbConn.commit()
            dbConn.close()
            collector.close()    
                    
            dbConn = getDbConnectInfo()       
            dbCursor = dbConn.cursor()        
            setLogPrint("@@@@@@@@@@ DB Connection = Close > Connect : "+str(VAR_DB_DEFAULT_COUNT)+" count")  
            
            collector = WebScraper()
            
            collector.setup_driver(headless=True)
            setLogPrint("@@@@@@@@@@ Selenium = Close > Connect : "+str(VAR_DB_DEFAULT_COUNT)+" count")
            
            #driver re setup
            indexUrl = INDEX_SAMPLE_URL            
            collector.connectToObject(indexUrl)
            
            # 4. 쿠키 및 세션, 로컬 스토리지 수집
            cookies = collector.get_cookies()
            session_storage = collector.get_session_storage()
            local_storage = collector.get_local_storage()
            
            # 5. 헤더 생성
            headers = collector.create_headers_with_cookies()

            VAR_PAGE_BLOCK +=1
            
    except Exception as e:
        print("mainProcess START","="*150)
        setLogPrint(f"오류 발생: {e}")        
        setLogPrint("="*50)
        setLogPrint("전체 Traceback:")
        traceback.print_exc()  
        print("mainProcess END","="*150)
        collector.close()
        dbCursor.close() 
        dbConn.commit()
        dbConn.close()   

def add_days_to_date(date_str, days):
    """날짜 문자열에 일수를 더하는 함수"""
    # 문자열을 datetime 객체로 변환
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # 일수 더하기
    new_date = date_obj + timedelta(days=days)
    
    # 다시 문자열로 변환
    return new_date.strftime('%Y-%m-%d')


if __name__ == "__main__":
    
    LOG_START_TIME = datetime.now()    
    now = datetime.now()
    
    if len(sys.argv) > 1:
        slsCode = sys.argv[1].replace("_","")        
    else:
        slsCode = "212"
        
    LOG_AREA_CODE = getAreaCode(slsCode)
    LOG_AREA_NAME = getAreaName(slsCode)
    LOG_SLS_CODE =slsCode

    setBatchExecLog("START",LOG_START_TIME.strftime('%Y-%m-%d')+"_"+str(LOG_AREA_CODE), LOG_SLS_CODE, LOG_AREA_CODE,0,0, str(LOG_SLS_CODE)+" Batch Start")
            
    PARAM_START_DAYS = 0
    PARAM_END_DAYS = 30
    FOR_COUNT = 0
    
    for i in range(1, PARAM_END_DAYS):
        
        FOR_COUNT+=1                
        FOR_START_TIME = datetime.now()
        
        #기본 검색: 오늘날짜+2 ~ 오늘날짜+3 
        TMP_START_DATE = now + timedelta(days=(PARAM_START_DAYS+i))
        TMP_END_DATE = now + timedelta(days=(PARAM_START_DAYS+(i+1)))        

        VAR_START_DATE = TMP_START_DATE.strftime('%Y-%m-%d')     #렌트 시작 날짜 : YYYY-MM-DD  
        VAR_START_TIME = "10:00:00"                                 #렌트 시작 시간: HH:MM:SS
        
        VAR_END_DATE = TMP_END_DATE.strftime('%Y-%m-%d')         #렌트 종료 날짜 : YYYY-MM-DD        
        VAR_END_TIME = "10:00:00"                                   #렌트 종료 시간 : HH:MM:SS
        
        # # 인자가 있는지 확인        
        # if len(sys.argv) > 2:
        #     startDate = add_days_to_date(sys.argv[2],0)
        #     endDate = add_days_to_date(sys.argv[2],1)
        #     print(f"StartDate: {startDate}")
        # else:
        #     startDate = VAR_START_DATE
        #     endDate = VAR_END_DATE                
        # startTime = VAR_START_TIME
        # endTime = VAR_END_TIME    
        
        # 5: "제주국제공항",1
        # 7: "김포국제공항",
        # 2: "김해국제공항",
        # 1: "인천국제공항%201터미널",
        # 212: "인천국제공항%202터미널",
        # 18: "서울역",
        # 21: "영등포역",
        # 37: "경주역",
        # 19: "순천역",
        # 20: "여수엑스포역",
        # 26: "부산역"
        
        setBatchExecLog("MAIN_FUNCTION_START",LOG_START_TIME.strftime('%Y-%m-%d')+"_"+str(LOG_AREA_CODE),LOG_SLS_CODE, LOG_AREA_CODE, i,PARAM_END_DAYS, f"{LOG_AREA_CODE} Batch > IN > {i}/{PARAM_END_DAYS} Start")        
        mainProcess(VAR_START_DATE,VAR_START_TIME, VAR_END_DATE, VAR_END_TIME, LOG_SLS_CODE, LOG_AREA_CODE, LOG_AREA_NAME )
        setBatchExecLog("MAIN_FUNCTION_END",LOG_START_TIME.strftime('%Y-%m-%d')+"_"+str(LOG_AREA_CODE),LOG_SLS_CODE, LOG_AREA_CODE, i,PARAM_END_DAYS, f"{LOG_AREA_CODE} Batch > IN > {i}/{PARAM_END_DAYS} End")        
        
        FOR_END_TIME = datetime.now()
        FOR_TIME_GAP = FOR_END_TIME - FOR_START_TIME
        elapsed_minutes = FOR_TIME_GAP.total_seconds()/60
        gapTime = f"{elapsed_minutes:.2f}"       
      
    LOG_END_TIME = datetime.now()
    TIME_GAP = LOG_END_TIME - LOG_START_TIME
    elapsed_minutes = TIME_GAP.total_seconds()/60
    gapTime = f"{elapsed_minutes:.2f}"
    
    setBatchExecLog("END", LOG_START_TIME.strftime('%Y-%m-%d')+"_"+str(LOG_AREA_CODE), LOG_SLS_CODE, LOG_AREA_CODE, 0,0, str(LOG_AREA_CODE)+" Batch END")
    setLogPrint("######## TOTAL END 소요시간:"+str(gapTime)+"분 ############")
    
