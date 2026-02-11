'''
KB Chachacha 사이트 수집 기능 (Multi Thread 이용)
# First Make Date 2025.10.31
- VIP 정보 가져오기 (차량번호 -> 오토비긴즈 연동)

'''
import os
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import json
import pymysql
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import random
import varList
import requests
import re
from requests.exceptions import HTTPError, Timeout, RequestException
import threading
from queue import Queue
from typing import List
import traceback

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


INDEX_SAMPLE_URL = "https://www.kbchachacha.com/public/search/main.kbc"
TARGET_SITE_LP_URL = "https://www.kbchachacha.com/public/search/list.empty?page=VAR_PAGE_NUM&sort=-orderDate"
#CARINFO_SYNC_URL = "http://newcarapi.autoplus.co.kr:10000/carNumberGetData.php?carNumber=CAR_PLATE_NUMBER&vi_no=VI_NUMBER&apiDivide=1"
CARINFO_SYNC_URL = "http://ip.wbslog.com/cp.php?plateNumber=CAR_PLATE_NUMBER"

TARGET_SITE_VIP_URL=""
USER_AGENT_NAME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) chrome=141.0.7390.107 Safari/537.36"
#CHROME_DIRVER_LOC="D:\\chromedriver-win64\\chromedriver.exe"          #Windows version


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
4.# Proxy IP/PORT get
#사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
def getProxyIpOne():      
    setLogPrint("#getProxyIpOne exec") 
    #MSS서버 접속 정보
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
                FROM NIF_PROXY_LIST 
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
                UPDATE NIF_PROXY_LIST
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
# Log Print 
def setLogPrint(msg):
    logNowTime = datetime.now()
    logFormatted = logNowTime.strftime("%Y-%m-%d %H:%M:%S")
    print("## [",logFormatted,"][",str(msg),"]")

# ────────────────────────────────────────────────────────────────────────────────────────────
# html 태그 제거 함수
def getWebSpiderData(driver, url, max_retries=3):
    """
    웹 페이지 데이터를 안전하게 가져오는 함수
    
    Args:
        driver: Selenium WebDriver 객체
        url: 크롤링할 URL
        max_retries: 최대 재시도 횟수 (기본값: 3)
    
    Returns:
        str: 페이지 HTML 소스 또는 None (실패 시)
    """
    
    for attempt in range(max_retries):
        retHtml = None
        
        try:
            setLogPrint(f"[시도 {attempt + 1}/{max_retries}] URL 접속 중: {url}")
            
            # 페이지 로드 타임아웃 설정
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            
            # 페이지 이동
            driver.get(url)
            
            # 페이지 로드 후 안정화 대기
            time.sleep(2)
            
            # 필수 메타 태그 대기
            wait = WebDriverWait(driver, 30)
            setLogPrint("VIP-PAGE Finding...")
            
            element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:description"]'))
            )
            
            setLogPrint("LP-PAGE Find Success > meta[property='og:description']")
            
            # 추가 안정화 대기 (동적 콘텐츠 로딩)
            time.sleep(1)
            
            # HTML 소스 가져오기
            retHtml = driver.page_source
            
            if retHtml and len(retHtml) > 100:  # 최소 길이 검증
                setLogPrint(f"페이지 소스 가져오기 성공 (크기: {len(retHtml)} bytes)")
                return retHtml
            else:
                setLogPrint("경고: 페이지 소스가 비어있거나 너무 작음")
                if attempt < max_retries - 1:
                    continue
                    
        except TimeoutException as e:
            setLogPrint(f"TimeoutException: 페이지 로딩 타임아웃 발생 (시도 {attempt + 1}/{max_retries})")
            traceback.print_exc()
            
            # 페이지 로딩 강제 중단
            try:
                driver.execute_script("window.stop();")
                setLogPrint("페이지 로딩 강제 중단")
                
                # 그래도 현재 상태의 소스 가져오기 시도
                retHtml = driver.page_source
                if retHtml and len(retHtml) > 100:
                    setLogPrint("부분 로드된 페이지 소스 사용")
                    return retHtml
                    
            except Exception as stop_error:
                setLogPrint(f"페이지 중단 실패: {str(stop_error)}")
            
            # 재시도 전 대기
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3  # 점진적 대기 시간 증가
                setLogPrint(f"{wait_time}초 후 재시도...")
                time.sleep(wait_time)
                
                # 페이지 새로고침 시도
                try:
                    driver.refresh()
                    time.sleep(2)
                except:
                    pass
            
        except WebDriverException as e:
            setLogPrint(f"WebDriverException: 드라이버 오류 발생 (시도 {attempt + 1}/{max_retries})")
            setLogPrint(f"오류 내용: {str(e)}")
            traceback.print_exc()
            
            # 치명적인 오류일 경우 재시도 중단
            if "chrome not reachable" in str(e).lower() or "session deleted" in str(e).lower():
                setLogPrint("치명적인 드라이버 오류 - 재시도 중단")
                return None
            
            if attempt < max_retries - 1:
                time.sleep(3)
                
        except Exception as e:
            setLogPrint(f"Exception: 예상치 못한 오류 발생 (시도 {attempt + 1}/{max_retries})")
            setLogPrint(f"오류 내용: {str(e)}")
            traceback.print_exc()
            
            if attempt < max_retries - 1:
                time.sleep(3)
    
    # 모든 재시도 실패
    setLogPrint(f"최종 실패: {url} - {max_retries}번 시도 모두 실패")
    return None

def getRemoveHtmlTags(text):
    
    #html 제거 버전
    reData = BeautifulSoup(text, 'html.parser')
    json_re_data = reData.pre.text
    #clean = re.compile('<.*?>')
    #return re.sub(clean, '', text)   
    return json_re_data

def getRemoveLxmlTags(text):
    
    #html 를 객체로 만들어서 리턴하는 기능
    reData = BeautifulSoup(text, 'lxml')   
    return reData


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


def setMssDbUpdate(procType, carInfo, cursor):  
    try:       
        
        if procType == "1":
            carInfo["syncStatus"]="3"
            carInfo["syncText"]="VIP Complete"
            updateQuery = f'''
                UPDATE TBL_CAR_PRODUCT_LIST    
                SET
                    MOD_YMD         = DATE_FORMAT(NOW(), '%Y%m%d')
                    ,MOD_HOUR       = DATE_FORMAT(NOW(), '%H')                                                  
                    ,MOD_DATE       = NOW()
                    ,FUEL           = '{carInfo["fuel"]}'                    
                    ,PLATE_NUMBER   = '{carInfo["plateNumber"]}'
                    ,SYNC_STATUS    = '{carInfo["syncStatus"]}'
                    ,SYNC_TEXT      = '{carInfo["syncText"]}'                    
                WHERE STATUS = '1'
                AND SITE_CODE   = '{carInfo["siteCode"]}'
                AND CAR_ID      = '{carInfo["carId"]}'
                AND PRD_SEQ     = '{carInfo["prdSeq"]}' 
            '''
            setLogPrint("UPDATE[Find ApModelId] = carId:"+carInfo["carId"]+" | plateNumber: :"+carInfo["plateNumber"]+" | apModelId:"+carInfo["apModelId"])
        else:
            updateQuery = f'''
                UPDATE TBL_CAR_PRODUCT_LIST    
                SET
                    MOD_YMD         = DATE_FORMAT(NOW(), '%Y%m%d')
                    ,MOD_HOUR       = DATE_FORMAT(NOW(), '%H')                                                  
                    ,MOD_DATE       = NOW()
                    ,AP_MODEL_ID    = '{carInfo["apModelId"]}'
                    ,COLOR          = '{carInfo["color"]}'
                    ,NEW_PRICE      = '{carInfo["newPrice"]}'
                    ,MAKE_PRICE     = '{carInfo["makePrice"]}'
                    ,VIN_NUMBER     = '{carInfo["vinNumber"]}'
                    ,KIND           = '{carInfo["kind"]}'
                    ,DOMESTIC       = '{carInfo["domestic"]}'
                    ,MAKER          = '{carInfo["maker"]}'
                    ,MODEL          = '{carInfo["model"]}'
                    ,MODEL_DETAIL   = '{carInfo["modelDetail"]}'
                    ,GRADE          = '{carInfo["grade"]}'
                    ,GRADE_DETAIL   = '{carInfo["gradeDetail"]}'
                    ,FUEL           = '{carInfo["fuel"]}'
                    ,MISSION        = '{carInfo["mission"]}'
                    ,SYNC_STATUS    = '{carInfo["syncStatus"]}'
                    ,SYNC_TEXT      = '{carInfo["syncText"]}'
                    ,FULL_NAME      = '{carInfo["fullName"]}'
                    ,PLATE_NUMBER   = '{carInfo["plateNumber"]}'
                WHERE STATUS = '1'
                AND SITE_CODE   = '{carInfo["siteCode"]}'
                AND CAR_ID      = '{carInfo["carId"]}'
                AND PRD_SEQ     = '{carInfo["prdSeq"]}' 
            ''' 
            setLogPrint("UPDATE[carMartSync] = carId:"+carInfo["carId"]+" | plateNumber: :"+carInfo["plateNumber"]+" | apModelId:"+carInfo["apModelId"]+" | fullName:"+carInfo["fullName"])
        cursor.execute(updateQuery)   
    except Exception as e:
        traceback.print_exc()  
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))
        setLogPrint("error:carId:"+carInfo["carId"])
        setLogPrint("error:updateQuery:"+str(updateQuery))
        pass

def setMssDbUpdateAuctionClose(carInfo, cursor):  
    try:       
        updateQuery = f'''
            UPDATE TBL_CAR_PRODUCT_LIST    
            SET
                MOD_YMD         = DATE_FORMAT(NOW(), '%Y%m%d')
                ,MOD_HOUR       = DATE_FORMAT(NOW(), '%H')                                                  
                ,MOD_DATE       = NOW()              
                ,SYNC_STATUS    = '9'
                ,SYNC_TEXT      = '{carInfo["syncText"]}'
            WHERE STATUS = '1'
            AND SITE_CODE   = '{carInfo["siteCode"]}'
            AND CAR_ID      = '{carInfo["carId"]}'
            AND PRD_SEQ     = '{carInfo["prdSeq"]}' 
        '''   
        
        cursor.execute(updateQuery)   
        setLogPrint("UPDATE[Close/SoldOut] = carId:"+str(carInfo["carId"])+" | detailUrl: "+carInfo["detailUrl"])
    except Exception as e:
        traceback.print_exc()  
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))
        setLogPrint("error:carId:"+str(carInfo["carId"]))
        setLogPrint("error:updateQuery:"+str(updateQuery))
        pass
# ────────────────────────────────────────────────────────────────────────────────────────────
#---------------------------------------  크롤링 시작   ------------------------------------------
#1.selenium 실행
#1.1.프록시 아이피 가져오기
def getProxyWebdriverInfo():   
        
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
    
    objectUrl = INDEX_SAMPLE_URL
    
    #1.2.크롬드라이브 셋팅
    chrome_options = Options()
    #options.add_extension(proxy_plugin_path)
    chrome_options.add_argument(f"--user-agent={USER_AGENT_NAME}")
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    #chrome_options.add_argument("--force-device-scale-factor=1")
    #chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_experimental_option('prefs', {
        'profile.default_content_setting_values': {
            'mixed_script': 1,  # 안전하지 않은 스크립트 허용
            'insecure_content': 1  # 안전하지 않은 콘텐츠 허용
        }
    })

    
    # ────────────────────────────────────────────────────────────────────────────────────────────
    #1.3.Chrome 실행 및 로그인
    CHROME_DRIVER_PATH = Service(CHROME_DIRVER_FILE)
    driver = webdriver.Chrome(options=chrome_options, service = CHROME_DRIVER_PATH, seleniumwire_options = wire_options)
   
    try:        
        driver.get(objectUrl)
        driver.implicitly_wait(20)        
        time.sleep(5)       

        # 쿠키 추출
        cookies = driver.get_cookies()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
            
        # 헤더 설정
        headers = {            
            "Referer": varList.MAIN_INDEX_HOST_NAME,
            "Host": varList.MAIN_HOST_NAME,
        }

    except Exception as e:
        traceback.print_exc()  
        setLogPrint("error:driver:"+str(driver))
        setLogPrint(f"Error occurred: {e}")             
    
    setLogPrint("time.sleep: 9sec")
    time.sleep(9)
        
    return driver


#웹드라이버 로그인 초기화
def getWebDriverLoginProc(driver, loginDivide): 

    #heydear는 로그인하지만, KBchachacha경우 로그인이 없어서 Skip 처리함
    global VAR_WD_DEFAULT_COUNT
    global VAR_PAGE_NUM

    # loginDivide = 1 > first Login
    # loginDivide = 3 > relogin
    VAR_WD_DEFAULT_COUNT+=1
    driver = getProxyWebdriverInfo()

    return driver

#정규식
def getRegexpFromStr(str, regexptStr):
    #print("#getRegexpCarId exec") 
    retData = str.replace("\n", "").replace("\r", "").replace("\t", "")
    pattern = f'{regexptStr}'
    matches = re.search(pattern, str)
    if matches:
         # Extract the 8-digit number from the matched group
        retData = matches.group(1)
    return retData

# 오토비긴즈(카마트) 연동 -------------------------------------------------------------------------
def getCarSpecInfoAPI(driver, carInfo):
    
    carInfoJson = {}
    try:
        carMartUrl = CARINFO_SYNC_URL
        carMartUrlTemp = carMartUrl.replace("CAR_PLATE_NUMBER", carInfo["plateNumber"]);
        # carMartUrlTemp = carMartUrlTemp.replace("VI_NUMBER", "");
        #setLogPrint("LINK: carMartUrlTemp:"+carMartUrlTemp)
        
        driver.get(carMartUrlTemp)
        driver.set_page_load_timeout(5)  #5초이상 지나면 강제 타임아웃 처리
        retHtml = driver.page_source  
             
        driver.implicitly_wait(1)   
        retHtml = retHtml.replace("<html><head></head><body>","").replace("</body></html>","").replace("16\"알로이휠","16인치 알로이휠")
        retHtml = retHtml.replace("15\"알로이휠","15인치 알로이휠")
        retHtml = retHtml.replace("17\"알로이휠","17인치 알로이휠")
        retHtml = retHtml.replace("18\"알로이휠","18인치 알로이휠")
        retHtml = retHtml.replace("19\"알로이휠","19인치 알로이휠")
        retHtml = retHtml.replace("20\"알로이휠","20인치 알로이휠")       
                      
        carInfoJson = json.loads(retHtml)
        
        if carInfoJson["resultCode"] == "00":
            carInfo["apModelId"] = carInfoJson["resultData"]["apModelId"]               #카마트기준 모델아이디
            carInfo["color"] = carInfoJson["resultData"]["color"]                       #카마트기준 색상
            carInfo["newPrice"] = carInfoJson["resultData"]["newPrice"]                 #카마트기준 신차가
            carInfo["makePrice"] = carInfoJson["resultData"]["carMakePrice"]            #카마트기준 출고가        
            carInfo["vinNumber"] = carInfoJson["resultData"]["vinCode"]                 #카마트기준 차대번호
            carInfo["kind"] = carInfoJson["resultData"]["kindName"]                     #카마트기준 차종명
            carInfo["domestic"] = carInfoJson["resultData"]["carDomestic"]              #카마트기준 국산/수입 구분  
            carInfo["maker"] = carInfoJson["resultData"]["makerName"]                   #카마트기준 제조사         
            carInfo["model"] = carInfoJson["resultData"]["modelName"]                   #카마트기준 모델명
            carInfo["modelDetail"] = carInfoJson["resultData"]["modelDetailName"]       #카마트기준 세부 모델명
            carInfo["grade"]  = carInfoJson["resultData"]["gradeName"]                  #카마트기준 등급명
            carInfo["gradeDetail"]  = carInfoJson["resultData"]["gradeDetailName"]      #카마트기준 세부 등급명
            carInfo["years"]  = carInfoJson["resultData"]["carYear"]                      #카마트기준 년식
            carInfo["fuel"]  = carInfoJson["resultData"]["fuel"]                        #카마트기준 년식
            carInfo["mission"] = carInfoJson["resultData"]["gearBox"]                   #카마트기준 미션(기어)
            carInfo["syncStatus"]="3"
            carInfo["syncText"]="VIP Complete"
            setLogPrint("INFO: >>> CAR-MART SYNC SUCCESS - AP_MODEL_ID:"+carInfo["apModelId"]+" | MODEL: "+carInfo["model"]+" | PLATE_NUMBER: "+carInfo["plateNumber"])
        else:     
            carInfo["apModelId"] = ""                   #카마트기준 모델아이디
            carInfo["color"] = ""                       #카마트기준 색상
            carInfo["newPrice"] = ""                    #카마트기준 신차가
            carInfo["makePrice"] = ""                   #카마트기준 출고가        
            carInfo["vinNumber"] = ""                   #카마트기준 차대번호
            carInfo["kind"] = ""                        #카마트기준 차종명
            carInfo["domestic"] = ""                    #카마트기준 국산/수입 구분  
            carInfo["maker"] = ""                       #카마트기준 제조사         
            carInfo["model"] = ""                       #카마트기준 모델명
            carInfo["modelDetail"] = ""                 #카마트기준 세부 모델명
            carInfo["grade"]  = ""                      #카마트기준 등급명
            carInfo["gradeDetail"]  = ""                #카마트기준 세부 등급명
            carInfo["years"]  = ""                      #카마트기준 년식
            carInfo["fuel"]  = ""                       #카마트기준 년식
            carInfo["mission"] = ""                     #카마트기준 미션(기어)     
            carInfo["syncStatus"]="9"
            carInfo["syncText"]="CAR-MART Sync Fail(Not Found)"    
            setLogPrint("WARN: XXX CAR-MART Not Found -- PLATE_NUMBER: "+carInfo["plateNumber"])
    except Exception as e:
        traceback.print_exc()  
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))
        setLogPrint("LINK: carMartUrlTemp:"+carMartUrlTemp)
        setLogPrint("Exception returHtml:"+str(retHtml))        
        setLogPrint("-------------- CarMart:crawlling Error --------------")
        setLogPrint("######### CAR-MART-ERROR:"+str(carInfoJson))              
    except Timeout:
    # 타임아웃 재시도 로직/로그
        raise
    except HTTPError as e:
        # 상태코드별 처리
        print("HTTPError", e.response.status_code, e.response.text[:200])
        raise
    except ValueError as e:
        # JSON 아님/파싱 실패
        traceback.print_exc()  
        print("JSON 파싱 실패:", str(e))
        # 필요시 r.text로 원문 확인
        # print(r.text[:500])
        raise
    except RequestException as e:
        # 기타 네트워크 예외
        traceback.print_exc()  
        print("요청 실패:", e)
        raise
    return carInfo

# 처리할 작업 함수 (실제 로직으로 대체하세요)
def process_item(key: int):
    
    """
    수집처리하는 로직 입력 영역
    """
    # 예시: 간단한 처리 시뮬레이션
    time.sleep(0.01)  # 실제 작업으로 대체
    #------------------------ Thread 내 로직 시작 -----------------------------
    #------------------------ Thread 내 로직 종료 -----------------------------

    num = 2
    print(f"## GGD : {key}")
    for num in range(9):
        sum = key*num
        print(f"{key} X {num} = {sum}")
    print(f"------- End -------")
    #print(f"Thread {threading.current_thread().name}: Processing key {key}")
  

# 워커 스레드 함수
def worker(queue: Queue, results: List, lock: threading.Lock):

    #Queue 에서 처리할 업무를 꺼내오는 기능
    while True:
        key = queue.get()
        if key is None:  # 종료 신호
            queue.task_done()
            break
        
        try:
            result = process_item(key)
            # 결과를 안전하게 저장
            with lock:
                results.append((key, result))
        except Exception as e:
            print(f"Error processing key {key}: {e}")
        finally:
            queue.task_done()
            
#2.타겟 사이트 접속
try:
    
    carList = ""
    #2.1.로그인 페이지 접속  
    driver = getWebDriverLoginProc("",1)
    setLogPrint("WebSite Connect Complete > Crawlling Start") 
     
    i = 0
        
    dbConn = getDbConnectInfo()
    try:
        dbCursor = dbConn.cursor()   # 실패 시 예외 발생
        setLogPrint("INFO: DB Connect Complete")
    except Exception as e:    
        print(f"ERROR: DB Cursor Connect Fail {type(e).__name__}: {e}")
        
        
    #사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
    #BIDDING_END_DATE 가 지난 건은 조회대상 제외
    query = '''
       SELECT 
            PRD_SEQ
            ,CAR_ID            
            ,DETAIL_URL
            ,SITE_CODE            
            ,AP_MODEL_ID
        FROM TBL_CAR_PRODUCT_LIST
        WHERE SITE_CODE='2000'
        AND STATUS='1'
        AND SYNC_STATUS ='1'
        ORDER BY ADD_DATE ASC 
        LIMIT 100       
    '''

    connInfo = dbConn.cursor(pymysql.cursors.DictCursor)
    connInfo.execute(query)  #쿼리 실행
    result=connInfo.fetchall()
    
    df = pd.DataFrame(result)
    totalCount = len(df)
    setLogPrint("INFO: 수집대상 차량건수:"+str(totalCount)+"건 DB SELECTED")
    subCarListCount = 0
    if len(df) > 0:
        procCount = 0
 
        for idx, row in df.iterrows():
            # dict 형태라 키로 접근 가능
            
            try :
                carInfo = {}

                prdSeq = row['PRD_SEQ']
                detailUrl = row['DETAIL_URL']
                carId = row['CAR_ID']
                apModelId = row ['AP_MODEL_ID'] or ""
                
                carInfo["prdSeq"]=prdSeq
                carInfo["siteCode"]="2000" # 1000:encar, 2000:KBchachacha             
                carInfo["carId"]=carId    
                carInfo["apModelId"]=apModelId            

                #5페이지 마다 로그인 재접속, DB POOL 재접속
                if VAR_PAGE_BLOCK > 15:
                    VAR_PAGE_BLOCK = 0
                    VAR_DB_DEFAULT_COUNT+=1

                    dbCursor.close()
                    dbConn.commit()
                    dbConn.close()
                    dbConn = getDbConnectInfo()
                    dbCursor = dbConn.cursor()
                    
                    END_TIME = datetime.now()
                    TIME_GAP = END_TIME - START_TIME
                    elapsed_minutes = TIME_GAP.total_seconds()/60
                    gapTime = f"{elapsed_minutes:.2f}"
                    driver = getWebDriverLoginProc(driver,3)
                    setLogPrint("DB Close>Open : "+str(VAR_DB_DEFAULT_COUNT)+" count | Elapsed Time:"+gapTime+" minute")
                    
                VAR_PAGE_BLOCK +=1
                ## Thread Start
                getDetailData = getWebSpiderData(driver,detailUrl)
                if getDetailData.find("로봇여부 확인") > 0 :
                    secNumber = random.randint(6,8) 
                    setLogPrint(f">>> Robot Searching.... Wait time: {secNumber} Sec")
                    time.sleep(secNumber)
                    continue
                
                if getDetailData.find("원하시는 페이지를 찾을 수 없습니다") > 0:
                    carInfo["detailUrl"]=detailUrl
                    carInfo["syncStatus"]="9"
                    carInfo["syncText"]="차량이 존재하지않음(삭제됨)"
                    setLogPrint(f"[SOLD_OUT] 차량이 존재하지않음(삭제됨)")                
                    setMssDbUpdateAuctionClose(carInfo, dbCursor)
                    continue
                    
                if getDetailData.find("판매가 완료된 차량입니다.") > 0:
                    carInfo["detailUrl"]=detailUrl
                    carInfo["syncStatus"]="9"
                    carInfo["syncText"]="차량이 존재하지않음(삭제됨)"
                    
                    setLogPrint(f"[SOLD_OUT] 판매가 완료된 차량입니다")
                    setMssDbUpdateAuctionClose(carInfo, dbCursor)
                    continue
                
                meta_match = re.search(r'<meta\s+property="og:description"\s+content="([^"]+)"', getDetailData)
                plateNumber = getRegexpFromStr(getDetailData,"<meta property=\"og:description\" content=\"[ ]{0,10}\(([0-9ㄱ-ㅎ가-힣]{0,10})\)").strip()
                
                if meta_match:
                    content = meta_match.group(1)
                    # | 로 분리
                    parts = [p.strip() for p in content.split('|')]
                    
                    if len(parts) >= 4:
                        # 1. fullName 추출
                        fullName_match = re.search(r'\)(.+)', parts[0])
                        fullName = fullName_match.group(1).strip() if fullName_match else ""
                        
                        # 2. years 추출 및 변환
                        years_match = re.search(r'(\d{2})년형', parts[1])
                        years = f"20{years_match.group(1)}" if years_match else ""
                        
                        # 3. 연료 추출
                        fuel = parts[3].strip()
                procCount+=1
                
                carInfo["fuel"]=fuel
                carInfo["fullName"]=fullName
                carInfo["plateNumber"]=plateNumber
                if len(apModelId) > 0 :
                    carInfo["syncStatus"]="3"
                    # Model matching Complete            
                    setMssDbUpdate("1", carInfo, dbCursor)     
                else:
                    # Model matching need
                    carInfo = getCarSpecInfoAPI(driver, carInfo)
                    setMssDbUpdate("3", carInfo, dbCursor)                 
                dbConn.commit()
                
                ## Thread End
                END_TIME = datetime.now()
                TIME_GAP = END_TIME - START_TIME
                elapsed_minutes = TIME_GAP.total_seconds()/60
                gapTime = f"{elapsed_minutes:.2f}"

                procPer = round(procCount/totalCount*100,1)                
                print("------------------------------------------- NEXT Count:[Proc:",procCount,"/Total:",totalCount,"][진척율:",procPer,"%][경과시간: "+str(gapTime)+"분]-------------------------------------------")
                secNumber = random.randint(4,6) 
                setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
                time.sleep(secNumber)
            except Timeout:
                traceback.print_exc()  
                driver.quit()
                # 타임아웃 재시도 로직/로그
                print("Timeout")
                raise
            except HTTPError as e:
                driver.quit()
                traceback.print_exc()  
                # 상태코드별 처리                
                print("HTTPError", e.response.status_code, e.response.text[:200])
                raise
            except ValueError as e:
                driver.quit()
                traceback.print_exc()  
                # JSON 아님/파싱 실패
                setLogPrint("ValueError(파싱실패):"+str(e))                
                # 필요시 r.text로 원문 확인
                # print(r.text[:500])
                raise
            except RequestException as e:
                driver.quit()
                traceback.print_exc()  
                # 기타 네트워크 예외
                setLogPrint("RequestException:"+str(e))                
                raise
            except Exception as e:
                driver.quit()
                traceback.print_exc()                  
                setLogPrint("Exception Type:"+str(type(e).__name__))
                setLogPrint("Exception Message:"+str(e))
                setLogPrint("-------------- Main:crawlling Error --------------")    
                pass 
        driver.quit()
except Timeout:
    traceback.print_exc()  
    driver.quit()
# 타임아웃 재시도 로직/로그
    raise
except HTTPError as e:
    driver.quit()
    traceback.print_exc()  
    # 상태코드별 처리
    print("HTTPError", e.response.status_code, e.response.text[:200])
    raise
except ValueError as e:
    driver.quit()
    traceback.print_exc()  
    # JSON 아님/파싱 실패    
    setLogPrint("ValueError Message:"+str(e))
    # 필요시 r.text로 원문 확인
    # print(r.text[:500])
    raise
except RequestException as e:
    driver.quit()
    traceback.print_exc()  
    # 기타 네트워크 예외    
    setLogPrint("RequestException Message:"+str(e))
    raise
except Exception as e:
    driver.quit()
    traceback.print_exc()      
    setLogPrint("Exception Type:"+str(type(e).__name__))
    setLogPrint("Exception Message:"+str(e))
    setLogPrint("-------------- Main:crawlling Error --------------")    
    pass
driver.quit()
END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("######## END 소요시간:"+str(gapTime)+"분 ############")
                                        
                                         