'''
KB Chachacha 사이트 수집 기능
# First Make Date 2025.10.29
- LP 목록 가져오기
'''

import os
import zipfile
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import json
import pymysql
import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from datetime import datetime
import random
import varList
import requests
import re
from requests.exceptions import HTTPError, Timeout, RequestException
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


INDEX_SAMPLE_URL = varList.INDEX_SAMPLE_URL
TARGET_SITE_LP_URL = varList.TARGET_SITE_LP_URL

TARGET_SITE_DETAIL_URL= varList.TARGET_SITE_DETAIL_URL
USER_AGENT_NAME = varList.USER_AGENT_NAME

CHROME_DIRVER_LOC=""

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
def getWebSpiderData(driver, url):
    #print("#getWebSpiderData exec")
    retHtml = ""
    
    driver.get(url)        
        
    # 더보기 버튼 계속 클릭
    click_count = 0    
    try:            
        wait = WebDriverWait(driver, 30)
        setLogPrint("LP-PAGE Finding..")
        element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-in.type-wd-list"))
        ) 
        setLogPrint("LP-PAGE Find Success > div.list-in.type-wd-list")
    except TimeoutException:
        traceback.print_exc()
        setLogPrint("TimeoutException: Find Tag is Fail "+str(e))        
    except Exception as e:        
        traceback.print_exc()
        setLogPrint("Exception:  Find Tag is Fail"+str(e))        
    retHtml = driver.page_source   
    return retHtml

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

def getCarInfoFromDB(carInfo, cursor):
    
    try:
        query = f'''
            SELECT 
                DOMESTIC 
                ,KIND
                ,MAKER
                ,MODEL
                ,MODEL_DETAIL
                ,GRADE 
                ,GRADE_DETAIL
                ,COLOR
                ,MISSION
                ,AP_MODEL_ID     
                ,NEW_PRICE
                ,MAKE_PRICE		
            FROM TBL_CAR_PRODUCT_LIST
            WHERE SITE_CODE = '{carInfo["siteCode"]}'
            AND FULL_NAME = '{carInfo["fullName"]}'
            AND YEARS = '{carInfo["years"]}'            
            AND AP_MODEL_ID IS NOT NULL
            AND MAKER IS NOT NULL
            ORDER BY ADD_DATE DESC
            LIMIT 1
        '''        
        cursor.execute(query)
        result = cursor.fetchall()
       
        if result and len(result) > 0:
            row = result[0]
            carInfo["domestic"] = row.get("DOMESTIC", "") or ""
            carInfo["kind"] = row.get("KIND", "") or ""
            carInfo["maker"] = row.get("MAKER", "") or ""
            carInfo["model"] = row.get("MODEL", "") or ""
            carInfo["modelDetail"] = row.get("MODEL_DETAIL", "") or ""
            carInfo["grade"] = row.get("GRADE", "") or ""
            carInfo["gradeDetail"] = row.get("GRADE_DETAIL", "") or ""
            carInfo["color"] = row.get("COLOR", "") or ""
            carInfo["mission"] = row.get("MISSION", "") or ""
            carInfo["apModelId"] = row.get("AP_MODEL_ID", "") or ""            
            carInfo["newPrice"] = row.get("NEW_PRICE", "") or ""
            carInfo["makePrice"] = row.get("MAKE_PRICE", "") or ""  
        else:
            carInfo["domestic"] = ""
            carInfo["kind"] = ""
            carInfo["maker"] = ""
            carInfo["model"] = ""
            carInfo["modelDetail"] = ""
            carInfo["grade"] = ""
            carInfo["gradeDetail"] = ""
            carInfo["color"] = ""
            carInfo["mission"] = ""
            carInfo["apModelId"] = ""   
            carInfo["newPrice"] = ""
            carInfo["makePrice"] = ""                 
            
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))
        setLogPrint("error:carId:"+carInfo["carId"])
        setLogPrint("error:selectQuery:"+str(query))        
        pass
    finally:
        return carInfo
    
def setMssDbInsert(carInfo, cursor):  
    try:
        query = f''' 
            SELECT 
                COUNT(*) AS CNT 
            FROM TBL_CAR_PRODUCT_LIST 
            WHERE STATUS = '1'
            AND SYNC_STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID ='{carInfo["carId"]}'
           
        '''     
        #print("###query::",query)
        cursor.execute(query)
        result = cursor.fetchall()
        insertQuery = ""
        updateQuery = ""    
    
        if int(result[0]["CNT"]) == 0:
            insertQuery = f'''            
            INSERT INTO TBL_CAR_PRODUCT_LIST (
                SITE_CODE
                ,CAR_ID              
                ,MODEL_DETAIL_ORI                         
                ,YEARS
                ,FIRST_DATE
                ,KM            
                ,PRICE
                ,STATUS
                ,ADD_DATE			
                ,ADD_YMD
                ,ADD_HOUR
                ,FULL_NAME                 
                ,DETAIL_URL                
                ,SYNC_STATUS   
                ,SYNC_TEXT
                ,DOMESTIC
                ,KIND
                ,MAKER
                ,MODEL
                ,MODEL_DETAIL 
                ,GRADE
                ,GRADE_DETAIL                             
                ,COLOR                
                ,MISSION
                ,AP_MODEL_ID                
                ,NEW_PRICE
                ,MAKE_PRICE
            )VALUES( 
                '{carInfo["siteCode"]}'
                ,'{carInfo["carId"]}'
                ,'{carInfo["modelDetailOri"]}'                
                ,'{carInfo["years"]}'
                ,'{carInfo["firstDate"]}'
                ,'{carInfo["km"]}'
                ,'{carInfo["price"]}'
                ,'1'             
                ,NOW()
                ,DATE_FORMAT(NOW(), '%Y%m%d')
                ,DATE_FORMAT(NOW(), '%H')
                ,'{carInfo["fullName"]}'
                ,'{carInfo["detailUrl"]}'
                ,'1'
                ,'LP수집완료'   
                ,'{carInfo["domestic"]}'
                ,'{carInfo["kind"]}'
                ,'{carInfo["maker"]}'
                ,'{carInfo["model"]}'
                ,'{carInfo["modelDetail"]}'
                ,'{carInfo["grade"]}'
                ,'{carInfo["gradeDetail"]}'
                ,'{carInfo["color"]}'                
                ,'{carInfo["mission"]}'
                ,'{carInfo["apModelId"]}'
                ,'{carInfo["newPrice"]}'
                ,'{carInfo["makePrice"]}'
            )
            '''
            cursor.execute(insertQuery)            
            setLogPrint("INSERT = carId:"+carInfo["carId"]+" | apModelId: "+carInfo["apModelId"]+" | modelDetailOri:"+carInfo["modelDetailOri"]+" | detailUrl:"+carInfo["detailUrl"])
        else:
            updateQuery = f'''
                UPDATE TBL_CAR_PRODUCT_LIST    
                SET	
                    MODEL_DETAIL_ORI= '{carInfo["modelDetailOri"]}'
                    ,PRICE          = '{carInfo["price"]}'
                    ,YEARS          = '{carInfo["years"]}'
                    ,KM             = '{carInfo["km"]}'
                    ,FULL_NAME      = '{carInfo["fullName"]}'
                    ,MOD_YMD        = DATE_FORMAT(NOW(), '%Y%m%d')
                    ,MOD_HOUR       = DATE_FORMAT(NOW(), '%H')                   
                    ,FIRST_DATE     = '{carInfo["firstDate"]}' 
                    ,DETAIL_URL     = '{carInfo["detailUrl"]}' 
                    ,DOMESTIC       = '{carInfo["domestic"]}'
                    ,KIND           = '{carInfo["kind"]}'
                    ,MODEL          = '{carInfo["model"]}'
                    ,MODEL_DETAIL   = '{carInfo["modelDetail"]}'
                    ,GRADE          = '{carInfo["grade"]}'
                    ,GRADE_DETAIL   = '{carInfo["gradeDetail"]}'
                    ,COLOR          = '{carInfo["color"]}'
                    ,MISSION        = '{carInfo["mission"]}'
                    ,AP_MODEL_ID    = '{carInfo["apModelId"]}'
                    ,NEW_PRICE      = '{carInfo["newPrice"]}'
                    ,MAKE_PRICE     = '{carInfo["makePrice"]}'
                WHERE STATUS = '1'
                AND SYNC_STATUS = '1'
                AND SITE_CODE = '{carInfo["siteCode"]}'
                AND CAR_ID = '{carInfo["carId"]}' 
            '''
            
            cursor.execute(updateQuery)   
            setLogPrint("UPDATE = carId:"+carInfo["carId"]+" | apModelId: "+carInfo["apModelId"]+" | modelDetailOri:"+carInfo["modelDetailOri"]+" | detailUrl:"+carInfo["detailUrl"])
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))
        setLogPrint("error:carId:"+carInfo["carId"])
        setLogPrint("error:insertQuery:"+str(insertQuery))
        setLogPrint("error:updateQuery:"+str(updateQuery))
        pass
# ────────────────────────────────────────────────────────────────────────────────────────────
#------------------  크롤링 시작   ---------------------
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
def getRegexpCarId(str, regexptStr):
    #print("#getRegexpCarId exec") 
    retData = str.replace("\n", "").replace("\r", "").replace("\t", "")
    pattern = f'{regexptStr}'
    matches = re.search(pattern, str)
    if matches:
         # Extract the 8-digit number from the matched group
        retData = matches.group(1)
    return retData



#2.1.로그인 페이지 접속  
driver = getWebDriverLoginProc("",1)
setLogPrint("WebSite Connect Complete > Crawlling Start") 
try:    
    carList = ""
    i = 0
        
    dbConn = getDbConnectInfo()
    dbCursor = dbConn.cursor()   
    setLogPrint("DB Connect Complete")          

    while True:    
        
        try:
                                
            #5페이지 마다 로그인 재접속, DB POOL 재접속
            if VAR_PAGE_BLOCK > 5:
                VAR_PAGE_BLOCK = 0
                VAR_DB_DEFAULT_COUNT+=1
                            
                dbCursor.close()        
                dbConn.commit()
                dbConn.close()        
                dbConn = getDbConnectInfo()       
                dbCursor = dbConn.cursor()        
                setLogPrint("DB Close>Open : "+str(VAR_DB_DEFAULT_COUNT)+" count")             
                driver = getWebDriverLoginProc(driver,3)       
                
            VAR_PAGE_BLOCK +=1
            
            targetUrl = TARGET_SITE_LP_URL.replace('VAR_PAGE_NUM',str(VAR_PAGE_NUM))  
            
        
            #setLogPrint("targetUrl:"+targetUrl)
            #driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH),seleniumwire_options=wire_options, options=chrome_options)
            #wait = WebDriverWait(driver, 15)
            VAR_PAGE_NUM+=1
            
            getListData = getWebSpiderData(driver,targetUrl)
            
            soup = BeautifulSoup(getListData, "lxml")
                
            # 1) list-in 컨테이너 선택
            wrap = soup.select_one("div.list-in.type-wd-list")
            if not wrap:
                setLogPrint("list-in.type-wd-list 컨테이너를 찾지 못했습니다.")
                setLogPrint("targetUrl:"+targetUrl)
                break
            
            # 2) 직계 자식 .area 만 선택 (중첩 방지)
            areas = wrap.select("div.area")

            results = []
            for idx, area in enumerate(areas, 1):
                # (1) data-car-seq 또는 data-seq 추출
                areaCarSeq = area.get("data-car-seq") or area.get("data-seq")

                # (2) area 내부 텍스트 (공백 정리)
                areaTitle = " ".join(area.get_text(strip=True).split())

                # (3) area 내부 '원본 HTML 조각'이 필요하면:
                areaHtmlBody = area.decode_contents().strip()      
                areaHtmlBody = areaHtmlBody.replace("\n", "").replace("\r", "").replace("\t", "")
                modelName = area.find("strong", class_="tit").get_text(strip=True)
                carPrice = getRegexpCarId(areaHtmlBody,"<span class=\"price\">([0-9,]{0,10})<span class=\"unit\">").strip().replace(",","").replace(".","")
                carKm = getRegexpCarId(areaHtmlBody,"<span>([0-9,]{0,20})km</span>").strip().replace(",","").replace(".","")
                carYearFullText = getRegexpCarId(areaHtmlBody,"<div class=\"data-line\"><span>([0-9/식년형()]{0,50})</span>").strip().replace(",","").replace(".","")  
                carYear = getRegexpCarId(carYearFullText,"([0-9]{2})년형").strip().replace("/","-")
                if len(carYear) == 2 :
                    carYear = "20"+carYear
                
                firstDate = getRegexpCarId(carYearFullText,"([0-9]{2}\/[0-9]{2})식").strip().replace("/","-")
                if len(firstDate) == 5:
                    firstDate = "20"+getRegexpCarId(carYearFullText,"([0-9]{2}\/[0-9]{2})식").strip().replace("/","-")+"-01"
                detailUrl = varList.MAIN_DETAIL_URL+"?carSeq="+areaCarSeq
                
                carInfo = {}
                carInfo["carId"]=areaCarSeq
                carInfo["siteCode"]="2000" # 1000:encar, 2000:KBchachacha             
                carInfo["modelDetailOri"]=modelName      
                carInfo["fullName"]=modelName
                carInfo["years"]=carYear           
                carInfo["km"]=carKm
                carInfo["price"]=carPrice           
                carInfo["firstDate"]=firstDate
                carInfo["detailUrl"]=detailUrl
                
                carInfo = getCarInfoFromDB(carInfo, dbCursor)
                setMssDbInsert(carInfo,dbCursor)                        
            dbConn.commit()
            print("TargetURL:"+targetUrl)
            print("---------------------------------- NEXT Page:[",VAR_PAGE_NUM,"]---------------------------------")
            secNumber = random.randint(8, 13) 
            setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
            time.sleep(secNumber)
        except Timeout:
            driver.quit()
            traceback.print_exc()  
            setLogPrint("Timeout")
            raise
        except HTTPError as e:
            driver.quit()
            traceback.print_exc()  
            setLogPrint("EHTTPErrorxception Type:"+str(type(e).__name__))
            setLogPrint("HTTPError Message:"+str(e))
            raise
        except ValueError as e:
            driver.quit()
            traceback.print_exc()  
            setLogPrint("ValueError Type:"+str(type(e).__name__))
            setLogPrint("ValueError Message:"+str(e))
            raise
        except RequestException as e:
            driver.quit()
            traceback.print_exc()  
            setLogPrint("RequestException Type:"+str(type(e).__name__))
            setLogPrint("RequestException Message:"+str(e))
            raise
        except Exception as e:
            traceback.print_exc()
            setLogPrint("Exception Type:"+str(type(e).__name__))
            setLogPrint("Exception Message:"+str(e))
            setLogPrint("-------------- Main:crawlling Error --------------")
            pass
    driver.quit()
except Timeout:
    driver.quit()
# 타임아웃 재시도 로직/로그
    raise
except HTTPError as e:
    driver.quit()
    # 상태코드별 처리
    print("HTTPError", e.response.status_code, e.response.text[:200])
    raise
except ValueError as e:
    driver.quit()
    # JSON 아님/파싱 실패
    print("JSON 파싱 실패:", str(e))
    # 필요시 r.text로 원문 확인
    # print(r.text[:500])
    raise
except RequestException as e:
    driver.quit()
    # 기타 네트워크 예외
    print("요청 실패:", e)
    raise
except Exception as e:
    driver.quit()    
    setLogPrint("Exception Type:"+str(type(e).__name__))
    setLogPrint("Exception Message:"+str(e))
    setLogPrint("-------------- Main:crawlling Error --------------")    
    pass
finally:
    driver.quit()
    
END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("######## END 소요시간:"+str(gapTime)+"분 ############")
                                        
                                         