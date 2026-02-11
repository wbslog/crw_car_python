'''
*@author: DoYeon.Shin(JinRoh)
*@date : 2025.10.14
*@desc : Heydealer Auction DetailPage(VIP) crawling (경매건 상세화면 수집)

*@기능정의
  - 상세화면 수집 대상 차량 DB에서 조회
  - 경매상세 화면 수집
  - 카마트 연동
  - 시세수집 상세링크 저장
'''

import os
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import pymysql
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import random
import traceback

# ────────────────────────────────────────────────────────────────────────────────────────────
BATCH_NAME = "BATCH2_VIP"

# 1.URL-Info, DB-Info
TARGET_SITE_LP_URL="https://api.heydealer.com/v2/dealers/web/cars/?page=VAR_PAGE_NUM&type=auction&is_subscribed=false&is_retried=false&is_previously_bid=false&order=default"
TARGET_SITE_DETAIL_URL="https://api.heydealer.com/v2/dealers/web/cars/VAR_HASH_ID/"          
#TARGET_SITE_PRICE_URL="https://dealer.heydealer.com/price?brand=xoKegB&grade=pAv7MV&max_mileage=60000&min_mileage=20000&model=23XlO3&model_group=zeXDJe&year=2020&year=2021&year=2022" 
#TARGET_SITE_PRICE_URL="https://dealer.heydealer.com/price?brand=VAR_BRAND_ID&grade=VAR_GRADE_ID&max_mileage=VAR_MAX_KM&min_mileage=VAR_MIN_KM&model=VAR_MODEL_ID&model_group=VAR_MODEL_GROUP_ID&year=VAR_YEAR1&year=VAR_YEAR2&year=VAR_YEAR3" 
TARGET_SITE_PRICE_URL="https://api.heydealer.com/v2/dealers/web/price/cars/?page=VAR_PAGE_NUM&model=VAR_MODEL_ID&grade=VAR_GRADE_ID&year=VAR_YEAR1&year=VAR_YEAR2&year=VAR_YEAR3&min_mileage=VAR_MIN_KM&max_mileage=VAR_MAX_KM&period=c&order=recent";
USER_AGENT_NAME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.7390.78 Safari/537.36"

CARINFO_SYNC_URL = "http://newcarapi.autoplus.co.kr:10000/carNumberGetData.php?carNumber=CAR_PLATE_NUMBER&vi_no=VI_NUMBER&apiDivide=1"

#CHROME_DIRVER_LOC="D:\\chromedriver-win64\\chromedriver.exe"          #Windows version
CHROME_DIRVER_LOC=""

DB_USER_ID="cwdb"
DB_USER_PASS="cwzmfhfflddb"
DB_SERVER_HOST="118.27.108.233"
DB_SERVER_PORT = 33066
DB_SERVER_NAME ="TS_CW_CARDB"
#OS구분 
#1: windwos, 3: Linux
if os.name == 'nt':
    CHROME_DIRVER_LOC="D:\\chromedriver-win64\\141.0.7390.76\\chromedriver.exe"          #Windows version
else:
    CHROME_DIRVER_LOC="/data/niffler_hd/chromedriver-linux64/chromedriver"    #Linux version

CHROMEDRIVER_PATH = CHROME_DIRVER_LOC

# ────────────────────────────────────────────────────────────────────────────────────────────
# 2. 프록시 설정
PROXY_HOST = ""
PROXY_PORT = ""
PROXY_USER = "madone"
PROXY_PASS = "trek1024!"

# ────────────────────────────────────────────────────────────────────────────────────────────
# 3. 로그인 정보
LOGIN_URL = "https://dealer.heydealer.com/"
LOGIN_ID = "economic82"
LOGIN_PW = "3531"
LOGIN_NAME="모원일"

#───────────────────────────────────────────────────────────────────────────────────────────
VAR_PAGE_NUM = 1
VAR_PAGE_BLOCK = 0
VAR_WD_DEFAULT_COUNT = 0
VAR_DB_DEFAULT_COUNT = 0
VAR_INSERT_COUNT = 0
VAR_UPDATE_COUNT = 0

VAR_SITE_CODE = "3"

START_TIME = datetime.now()

# ────────────────────────────────────────────────────────────────────────────────────────────
4.# Proxy IP/PORT get
#사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
def getProxyIpOne():      
    setLogPrint("INFO: getProxyIpOne exec") 
    #CAR_DB 서버 접속 정보
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
    proxyInfo["PROXY_USER_ID"]=PROXY_USER
    proxyInfo["PROXY_USER_PASS"]=PROXY_PASS   
    
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
    
    setLogPrint("INFO: ProxyInfo:IP:"+proxyInfo["PROXY_IP"]+"/PORT:"+proxyInfo["PROXY_PORT"])
    
    return proxyInfo

# ────────────────────────────────────────────────────────────────────────────────────────────
# 인증 프록시 확장 플러그인 생성
# 미사용(2025.06.25)
def create_proxy_auth_plugin(host, port, user, pwd):
    """프록시 인증 플러그인 폴더 경로 생성"""
    import zipfile, os
    from string import Template

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "ProxyAuth",
        "permissions": ["proxy", "tabs", "unlimitedStorage", "storage", "<all_urls>", "webRequest", "webRequestBlocking"],
        "background": {"scripts": ["background.js"]},
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js_template = Template("""
    var config = {
        mode: "fixed_servers",
        rules: {
            singleProxy: {
                scheme: "http",
                host: "$host",
                port: parseInt($port)
            },
            bypassList: ["localhost"]
        }
    };
    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
    chrome.webRequest.onAuthRequired.addListener(
        function(details) {
            return {authCredentials: {username: "$user", password: "$pwd"}};
        },
        {urls: ["<all_urls>"]},
        ['blocking']
    );
    """)

    background_js = background_js_template.substitute(host=host, port=port, user=user, pwd=pwd)

    pluginfile = f'proxy_auth_plugin_{user}_{port}.zip'
    with zipfile.ZipFile(pluginfile, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)

    return os.path.abspath(pluginfile)

# ────────────────────────────────────────────────────────────────────────────────────────────
# Log Print 
def setLogPrint(msg):
    
    '''
    [ 2025-10-15 08:26:29 ][ WARN: DB CONNECT ERROR ]           <<  오류 또는 경고문구출력시
    [ 2025-10-15 08:26:29 ][ INFO: SELECT DB DATA SUCCESS ]     << 일반적인 정보 출력시
    [ 2025-10-15 08:26:29 ][ TIME: time.Sleep:: 3 Sec ]         << 타임 또는 시간관련 정보 출력시
    [ 2025-10-15 08:26:32 ][ LINK: carMartUrlTemp @ http://domain/ ]   << 링크 출력시
    '''
    
    logNowTime = datetime.now()
    logFormatted = logNowTime.strftime("%Y-%m-%d %H:%M:%S")
    print("[",logFormatted,"]["+BATCH_NAME+"][",str(msg),"]")

# ────────────────────────────────────────────────────────────────────────────────────────────
# html 태그 제거 함수
def getWebSpiderData(driver, url):
    #print("#getWebSpiderData exec")
    retHtml = ""    
    try:
        driver.get(url)
        retHtml = driver.page_source        
        driver.implicitly_wait(10)       
    except Exception as e:
        setLogPrint(f"ERROR LINK:"+url)
        setLogPrint(f"ERROR: Exception:{e}")
        setLogPrint(f"ERROR: retHtml:"+str(retHtml))
        pass    
    return retHtml

def getRemoveHtmlTags(text):
    reData = BeautifulSoup(text, 'html.parser')
    json_re_data = reData.pre.text
    #clean = re.compile('<.*?>')
    #return re.sub(clean, '', text)   
    return json_re_data

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


def setCarInfoBiddingCloseUpdate(prdSeq,carId, siteCode,  curSor, syncText):
    
    updateQuery =""
    try:
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET
                 MOD_DATE = NOW()
                ,SYNC_STATUS ='2'
                ,SYNC_TEXT = '{syncText}'
            WHERE STATUS = '1'
            AND SITE_CODE = '{siteCode}'
            AND PRD_SEQ = '{prdSeq}' 
        '''        
        curSor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("INFO: UPDATE TBL_CAR_AUCTION_LIST = carId: "+str(carId)+"  |  PRD_SEQ:"+str(prdSeq)+"  |  SYNC_TEXT="+syncText)
    except Exception as e:
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))        
        setLogPrint("ERROR: error:carId: ["+str({"carId"})+"]-updateQuery:"+str(updateQuery))
        pass

def setCarInfoDbUpdate(carInfo, cursor):
    try:
        updateQuery = ""
        
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET
                VIN_NUMBER = '{carInfo["vinNumber"]}'
                ,PLATE_NUMBER = '{carInfo["plateNumber"]}'
                --  ,ACA_NUMBER = ''
                --  ,ACA_ROUND = ''
                ,DOMESTIC = '{carInfo["domestic"]}'                
                ,KIND_ORI = '{carInfo["kindOri"]}'   
                ,MAKER_ORI = '{carInfo["makerOri"]}'   
                ,MODEL_ORI = '{carInfo["modelOri"]}'   
                ,MODEL_DETAIL_ORI = '{carInfo["modelDetailOri"]}'   
                ,GRADE_ORI = '{carInfo["gradeOri"]}'   
                ,GRADE_DETAIL_ORI = '{carInfo["gradeDetailOri"]}'   
                ,COLOR_ORI = '{carInfo["colorOri"]}'   
                ,MISSION_ORI = '{carInfo["missionOri"]}'   
                ,FUEL_ORI = '{carInfo["fuelOri"]}'
                ,KIND = '{carInfo["kind"]}'
                ,MAKER = '{carInfo["maker"]}'
                ,MODEL = '{carInfo["model"]}'
                ,MODEL_DETAIL = '{carInfo["modelDetail"]}'
                ,GRADE= '{carInfo["grade"]}'
                ,GRADE_DETAIL = '{carInfo["gradeDetail"]}'
                ,COLOR= '{carInfo["color"]}'
                ,MISSION= '{carInfo["mission"]}'
                ,FUEL = '{carInfo["fuel"]}' 
                ,YEARS = '{carInfo["years"]}'               
                ,KM = '{carInfo["km"]}'           
                ,CC = '{carInfo["cc"]}'                
                ,FULL_NAME = '{carInfo["fullName"]}'
                ,REG_DATE = '{carInfo["regDate"]}' 
                ,AP_MODEL_ID = '{carInfo["apModelId"]}'
                -- ,ETC_TEXT = '{carInfo["etcText"]}'
                -- ,CHECK_TEXT = '{carInfo["checkText"]}'
                ,EVAL_TEXT = '{carInfo["evalText"]}'
                ,NEW_PRICE = '{str(carInfo["newPrice"])}'
                ,MAKE_PRICE = '{str(carInfo["makePrice"])}'
                ,FIRST_DATE = '{carInfo["firstDate"]}'
                -- ,EVAL_GRADE = ''              
                -- ,ACA_ROUND_DATE = ''
                -- ,START_PRICE = ''
                ,BIDDING_PRICE  = '{str(carInfo["biddingPrice"])}'
                ,BIDDING_STATUS = '{carInfo["biddingStatus"]}'                
                ,MAX_BIDDING_COUNT = '{carInfo["maxBiddingCount"]}'
                ,BIDDING_COUNT = '{carInfo["biddingCount"]}'
                ,MOD_DATE = NOW()    
                ,MOD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')
                ,MOD_HOUR = DATE_FORMAT(NOW(), '%H')
                ,PRICE_LIST_URL = '{carInfo["priceListUrl"]}'
                ,SYNC_STATUS ='2'
                ,SYNC_TEXT = '{carInfo["syncText"]}'
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID = '{carInfo["carId"]}' 
            AND SYNC_STATUS = '1'
        '''        
        cursor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("INFO: UPDATE TBL_CAR_AUCTION_LIST = carId: "+str(carInfo["carId"])+" | biddingPrice:"+str(carInfo["biddingPrice"])+" | biddingStatus:"+str(carInfo["biddingStatus"]))
    except Exception as e:
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))    
        traceback.print_exc()  # 전체 스택 추적 출력    
        setLogPrint("ERROR: error:carId: ["+str({carInfo["carId"]})+"]--------updateQuery:"+str(updateQuery))
        pass
# ────────────────────────────────────────────────────────────────────────────────────────────

#웹드라이버 로그인 초기화
def getWebDriverLoginProc(driver, loginDivide):
    global VAR_WD_DEFAULT_COUNT
    global VAR_PAGE_NUM

    # loginDivide = 1 > first Login
    # loginDivide = 3 > relogin
    VAR_WD_DEFAULT_COUNT+=1

    if loginDivide == 1:
        setLogPrint("INFO: WebDriver ["+str(VAR_WD_DEFAULT_COUNT)+"] 첫번째 START")
        driver = getProxyWebdriverInfo()
        wait = WebDriverWait(driver, 15)

        driver.get(LOGIN_URL)
        setLogPrint("LINK: LOGIN_URL:"+LOGIN_URL)

        #2.2.로그인 입력
        wait.until(EC.presence_of_element_located((By.NAME, "username"))).send_keys(LOGIN_ID)
        driver.find_element(By.NAME, "password").send_keys(LOGIN_PW)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        #2.3.로그인 성공 확인
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '로그아웃')]")))
        setLogPrint("INFO: [✓]최초 로그인 처리 성공 ++++")
        secNumber = random.randint(3, 6)
        setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    else:
        driver.quit()
        setLogPrint("INFO: WebDriver ["+str(VAR_WD_DEFAULT_COUNT)+"]번쩨 초기화 START")
        driver = getProxyWebdriverInfo()
        wait = WebDriverWait(driver, 15)

        driver.get(LOGIN_URL)
        setLogPrint("LINK: LOGIN_URL:"+LOGIN_URL)

        #2.2.로그인 입력
        wait.until(EC.presence_of_element_located((By.NAME, "username"))).send_keys(LOGIN_ID)
        driver.find_element(By.NAME, "password").send_keys(LOGIN_PW)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        #2.3.로그인 성공 확인
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '로그아웃')]")))
        setLogPrint("INFO: [✓]["+str(VAR_WD_DEFAULT_COUNT)+"]번째 로그인 처리 성공 ++++")
        secNumber = random.randint(3, 6)
        setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    return driver

# 오토비긴즈(카마트) 연동 -------------------------------------------------------------------------
def getCarSpecInfoAPI(driver, carInfo):
    
    carMartUrl = CARINFO_SYNC_URL
    carMartUrlTemp = carMartUrl.replace("CAR_PLATE_NUMBER", carInfo["plateNumber"]);
    carMartUrlTemp = carMartUrlTemp.replace("VI_NUMBER", "");
    #setLogPrint("LINK: carMartUrlTemp:"+carMartUrlTemp)
    
    driver.get(carMartUrlTemp)
    retHtml = driver.page_source        
    driver.implicitly_wait(1)   
    retHtml = retHtml.replace("<html><head></head><body>","").replace("</body></html>","").replace("16\"알로이휠","16인치 알로이휠")
    
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
        setLogPrint("INFO: >>> CAR-MART SYNC SUCCESS - AP_MODEL_ID:"+carInfo["apModelId"]+" | MODEL: "+carInfo["model"])
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
        setLogPrint("WARN: XXX CAR-MART Not Found Car-Spec")
    return carInfo

#------------------  크롤링 시작   ---------------------
#1.selenium 실행
#1.1.프록시 아이피 가져오기
def getProxyWebdriverInfo():

    proxyInfoMap = getProxyIpOne()
    PROXY_HOST = proxyInfoMap["PROXY_IP"]
    PROXY_PORT = proxyInfoMap["PROXY_PORT"]

    wire_options = {
        'proxy': {
            'http': f'socks5://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}',
            'https': f'socks5://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}',
            'no_proxy': 'localhost,127.0.0.1'
        }
    }
        
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
    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH),seleniumwire_options=wire_options, options=chrome_options)
    setLogPrint("INFO: getProxyWebdriverInfo  성공")
    secNumber = random.randint(3, 6)
    setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
    time.sleep(secNumber)
    return driver

#2.타겟 사이트 접속
    
try:
    driver = getWebDriverLoginProc("",1)
    
    #DB에서 상세화면 크롤링 목록 가져오기
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
            ,BIDDING_END_DATE
            ,PRICE_LIST_URL
        FROM TBL_CAR_AUCTION_LIST
        WHERE SITE_CODE = '3'
        AND SYNC_STATUS ='1'        
        AND BIDDING_END_DATE  >= DATE_FORMAT(NOW(),'%Y-%m-%d' )
        AND ( PRICE_LIST_URL = '' OR PRICE_LIST_URL IS NULL  )
        ORDER BY REG_DATE ASC
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
            prdSeq = row['PRD_SEQ']
            detailUrl = row['DETAIL_URL']
            carId = row['CAR_ID']
            biddingEndDate = row['BIDDING_END_DATE']
            
            # 2.1.로그인 페이지 접속
            # 5 페이지 마다 로그인 재접속, DB POOL 재접속
            if VAR_PAGE_BLOCK > 17:
                VAR_PAGE_BLOCK = 0
                VAR_DB_DEFAULT_COUNT+=1

                dbCursor.close()
                dbConn.commit()
                dbConn.close()
                dbConn = getDbConnectInfo()
                dbCursor = dbConn.cursor()
                setLogPrint("INFO: DB 연결종료 > 재연결 : "+str(VAR_DB_DEFAULT_COUNT)+" 번째")
                driver = getWebDriverLoginProc(driver,3)
                
            VAR_PAGE_BLOCK+=1
            procCount+=1

            targetUrl = TARGET_SITE_DETAIL_URL.replace("VAR_HASH_ID",carId)      
            getListData = getRemoveHtmlTags(getWebSpiderData(driver,targetUrl))
            setLogPrint("LINK: DetailUrl:"+targetUrl)
            
            secNumber = 1
            setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
            time.sleep(secNumber)   
            
            carList = json.loads(getListData)
            
            #print("--------------------------------------------carList------------------------------------------------")
            #setLogPrint("@@@@"+str(carList))
            ## 로그인이 풀린경우 중지시킴
            # {"code":null,"message":null,"toast":{"message":"로그인 후 사용해주세요.","type":"default"},"popup":null,"toast_message":"로그인 후 사용해주세요."}
            if "toast_message" in carList:
                if carList.get("toast_message").find("로그인 후 사용") > -1 :
                    setLogPrint("ERROR: Duplicate login detected and stopped")
                    exit(0)
                else:
                    if carList.get("toast_message").find("권한이 없습니다") > -1 :
                        setLogPrint("INFO: XXX 경매건이 존재하지 않음( VIP Not Found Page - carId: "+str(carId)+"]BiddingEndDate:"+str(biddingEndDate))
                        setCarInfoBiddingCloseUpdate(prdSeq,carId, VAR_SITE_CODE, dbCursor, "상세화면 없음(HD-VIP)")
                        setLogPrint("--------------------------------------------------- NEXT1 ---------------------------------------------------")
                    else:
                        setLogPrint("INFO: 경매 상세화면 있음 (VIP Find It - carId: "+str(carId) +")" )                   
                        setLogPrint("--------------------------------------------------- NEXT2 ---------------------------------------------------")                             
            else:   
                carInfo = {}
                detail = carList.get("detail", {})
                auction = carList.get("auction", {})
                etc = carList.get("etc", {})

                etcText=""  #
                checkText = "" #체크내역
                evalText ="" #점검내역
                conditionList = "" #상태정보
                carhistoryList = "" #
                carSubInfo= ""

                #평가정보
                conditionLists = detail.get("condition_description_items")
                for i,items in enumerate(conditionLists):
                    conditionList+=items.get("text")+"|"
                evalText = conditionList

                #보험처리(사고이력)
                carhistoryInfo = detail.get("carhistory", {})

                carInfo["kindOri"] = carhistoryInfo.get("car_type")

                carInfo["cc"] = carhistoryInfo.get("displacement")
                carInfo["modelOri"] = carhistoryInfo.get("model_group")
                
                carInfo["myCarAccidentCount"] = carhistoryInfo.get("my_car_accident_count")
                carInfo["otherCarAccidentCount"] = carhistoryInfo.get("other_car_accident_count")
                carInfo["myCarAccidentCost"] = carhistoryInfo.get("my_car_accident_cost")
                carInfo["otherCarAccidentCost"] = carhistoryInfo.get("other_car_accident_cost")               
                
                carInfo["plateNumber"]=detail.get("car_number")      
                carInfo["carId"]=carList.get("hash_id")
                carInfo["detailHashId"]=detail.get("detail_hash_id")
                carInfo["modelHashId"]=detail.get("model_hash_id")
                carInfo["siteCode"]=VAR_SITE_CODE  # 1: glovice, 3: HeyDealer zero 서비스              
                carInfo["conditions"]=""
                carInfo["checkText"]=checkText.replace("'","''")
                carInfo["etcText"]=etcText.replace("'","''")
                carInfo["evalText"]=evalText.replace("'","''")
                carInfo["trust"]=""
                carInfo["makerOri"]=detail.get("brand_name")
                carInfo["modelOri"]=""
                carInfo["modelDetailOri"]=detail.get("model_part_name")
                carInfo["gradeOri"]=""
                carInfo["gradeDetailOri"]=detail.get("grade_part_name")
                carInfo["fullName"]=detail.get("full_name")
                carInfo["missionOri"]=detail.get("transmission_display")
                carInfo["fuelOri"]=detail.get("fuel_display")
                carInfo["years"]=str(detail.get('year'))                
                carInfo["yearsType"]=""
                carInfo["km"]=str(detail.get('mileage'))
                carInfo["price"]=""
                carInfo["addArea"]=detail.get('short_location_first_part_name')
                carInfo["ModifiedDate"]=""
                carInfo["LeaseType"]=""
                carInfo["colorOri"]=detail.get('color')

                carInfo["firstDate"]=detail.get('initial_registration_date')[0:10]
                carInfo["regDate"]=auction.get("approved_at")[0:10]
                carInfo["biddingStatus"]=carList.get("status_display")
                carInfo["status"]=carList.get("status")
                carInfo["maxBiddingCount"]=str(auction.get("max_bids_count"))
                carInfo["biddingCount"]=str(auction.get("bids_count"))            
                carInfo["newPrice"]=detail.get('standard_new_car_price')
                carInfo["biddingPrice"] = "0"
                carInfo["syncText"] = "시세수집대기(HD-VIP)"
            
                etcData = etc.get("price_info",{}).get("params",{})             
                                
                #print("--------------------------------------------etcData------------------------------------------------")                
                #print("@@@@"+str(etcData))
                targetSubUrl = ""
                if len(etcData) > 0 :
                    subCarListCount = 0
                    years = etcData.get("year", [])
                    #years값이 없는경우 예외 처리 필요(2025.08.01)
                    #replace 인사값이 문자가 아닌 숫자가 들어가서 오류남 (주행거리 등등.)
                    carInfo["year1"] = years[0]
                    carInfo["year2"] = years[1]
                    carInfo["year3"] = years[2]
                    carInfo["minKm"] = etcData.get("min_mileage")
                    carInfo["maxKm"] = etcData.get("max_mileage")
                    carInfo["brandId"] = etcData.get("brand")
                    carInfo["modelGroupId"] = etcData.get("model_group")
                    carInfo["modelId"] = etcData.get("model")
                    carInfo["gradeId"] = etcData.get("grade")
                
                    targetSubUrl = TARGET_SITE_PRICE_URL  
                    targetSubUrl = targetSubUrl.replace("VAR_MODEL_ID",etcData.get("model")) 
                    targetSubUrl = targetSubUrl.replace("VAR_GRADE_ID",etcData.get("grade")) 
                    targetSubUrl = targetSubUrl.replace("VAR_YEAR1",str(years[0]))
                    targetSubUrl = targetSubUrl.replace("VAR_YEAR2",str(years[1]))
                    targetSubUrl = targetSubUrl.replace("VAR_YEAR3",str(years[2]))
                    
                    if str(etcData.get("min_mileage")) is not None:
                        targetSubUrl = targetSubUrl.replace("VAR_MIN_KM",str(etcData.get("min_mileage")))
                    else:
                        #targetSubUrl = targetSubUrl.replace("VAR_MIN_KM",int(detail.get('mileage'))-5000)
                        targetSubUrl = targetSubUrl.replace("VAR_MIN_KM","")
                    if str(etcData.get("max_mileage")) is not None:
                        targetSubUrl = targetSubUrl.replace("VAR_MAX_KM",str(etcData.get("max_mileage")))
                    else:
                        #targetSubUrl = targetSubUrl.replace("VAR_MAX_KM",int(detail.get('mileage'))+5000)
                        targetSubUrl = targetSubUrl.replace("VAR_MAX_KM","")
                    targetSubUrl = targetSubUrl.replace("None","")   
                    carInfo["priceListUrl"] = targetSubUrl
                else:  
                    carInfo["priceListUrl"] = ""
                    
                carInfo = getCarSpecInfoAPI(driver, carInfo)
                                
                setCarInfoDbUpdate(carInfo,dbCursor)                
                dbConn.commit()                  
                procPercent = round(procCount/totalCount*100,1)
                setLogPrint("INFO: >>> 처리진행상황  "+str(procCount)+"건 완료("+str(procPercent)+" %) / 전체 "+str(totalCount)+"건 - 차량번호:"+carInfo["plateNumber"]+" | carId: "+carInfo["carId"]+" <<<" )
                setLogPrint("--------------------------------------------------- NEXT3 ---------------------------------------------------")
                
    else:
        setLogPrint("INFO: XXX 수집대상 차량이 DB에 존재하지 않습니다.")
finally:
    connInfo.close() ## 연결 정도 자원 반환
    dbConn.close()

    df.head()
    if not df.empty:
        df.describe()

END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("INFO: Crawling End -- 수집 소요시간:"+str(gapTime)+"분 -- ")
