#2025.08.06: 오차범위가 낮은 1건 추출 함수 개발중
#  개발은 했고, 이식작업중
#@2025.08.10: update 쿼리에서 prd_sub_seq값이 널값이라 에러남.  미래의 신도연 고처라
#@2025.08.12: print 출력시 문자열 처리를 숫자로 잘못 처리한 유형 수정함

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
import varList

# ────────────────────────────────────────────────────────────────────────────────────────────
BATCH_NAME = "BATCH3_PLP"

# 1.URL-Info, DB-Info
TARGET_SITE_LP_URL=varList.TARGET_SITE_LP_URL
TARGET_SITE_DETAIL_URL=varList.TARGET_SITE_DETAIL_URL
TARGET_SITE_PRICE_URL=varList.TARGET_SITE_PRICE_URL
USER_AGENT_NAME =varList.USER_AGENT_NAME

CHROME_DIRVER_LOC=""

DB_USER_ID     = varList.DB_USER_ID
DB_USER_PASS   = varList.DB_USER_PASS
DB_SERVER_HOST = varList.DB_SERVER_HOST
DB_SERVER_PORT = varList.DB_SERVER_PORT
DB_SERVER_NAME = varList.DB_SERVER_NAME

#OS구분 
#1: windwos, 3: Linux
if os.name == 'nt':
    CHROME_DIRVER_LOC=varList.CHROME_DIRVER_LOC_NT
else:
    CHROME_DIRVER_LOC=varList.CHROME_DIRVER_LOC_LNX

CHROMEDRIVER_PATH = CHROME_DIRVER_LOC

# ────────────────────────────────────────────────────────────────────────────────────────────
# 2. 프록시 설정
PROXY_HOST = ""
PROXY_PORT = ""
PROXY_USER = varList.PROXY_USER
PROXY_PASS = varList.PROXY_PASS

# ────────────────────────────────────────────────────────────────────────────────────────────
# 3. 로그인 정보
LOGIN_URL = varList.LOGIN_URL
LOGIN_ID = varList.LOGIN_ID
LOGIN_PW = varList.LOGIN_PW
LOGIN_NAME= varList.LOGIN_NAME

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
    setLogPrint("#getProxyIpOne exec") 
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
    
    setLogPrint("ProxyInfo:IP:"+proxyInfo["PROXY_IP"]+"/PORT:"+proxyInfo["PROXY_PORT"])
    
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
    logNowTime = datetime.now()
    logFormatted = logNowTime.strftime("%Y-%m-%d %H:%M:%S")
    print("## [",logFormatted,"][",str(msg),"]")

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
        setLogPrint(f"error:url:"+url)
        setLogPrint(f"error:Exception:{e}")
        setLogPrint(f"error:retHtml:"+str(retHtml))
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

def setAuctionSubInsert(carInfo, cursor):
    try:
        insertQuery = "" 
        
        query = f''' 
            SELECT 
                COUNT(*) AS CNT 
            FROM TBL_CAR_AUCTION_SUB_LIST 
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID ='{carInfo["carId"]}'
            AND MODEL_ID ='{carInfo["modelId"]}'
            AND GRADE_ID ='{carInfo["gradeId"]}'
            AND YEARS ='{carInfo["years"]}'
            AND KM ='{carInfo["km"]}'
           
        '''
        #AND ADD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')  제거함
        #print("###query::",query)
        cursor.execute(query)
        result = cursor.fetchall()

        if int(result[0]["CNT"]) == 0:
            insertQuery = f'''
                INSERT INTO TBL_CAR_AUCTION_SUB_LIST(
                    CAR_ID
                    ,MODEL_ID
                    ,GRADE_ID
                    ,BIDDING_PRICE
                    ,BIDDING_COUNT
                    ,FUEL
                    ,GRADE_DETAIL
                    ,MISSION
                    ,YEARS
                    ,KM
                    ,FROM_END_DATE
                    ,MY_ACCIDENT_PRICE
                    ,MY_ACCIDENT_COUNT
                    ,OWNER_CHANGED_COUNT
                    ,ACCIDENT_LIST
                    ,ADD_DATE
                    ,STATUS
                    ,SITE_CODE
                ) VALUES (
                    '{carInfo["carId"]}'
                    ,'{carInfo["modelId"]}'
                    ,'{carInfo["gradeId"]}'
                    ,'{carInfo["biddingPrice"]}'
                    ,'{carInfo["biddingCount"]}'
                    ,'{carInfo["fuel"]}'
                    ,'{carInfo["gradeDetail"]}'
                    ,'{carInfo["mission"]}'
                    ,'{carInfo["years"]}'
                    ,'{carInfo["km"]}'
                    ,'{carInfo["fromEndDate"]}'
                    ,'{carInfo["myAccidentPrice"]}'
                    ,'{carInfo["myAccidentCount"]}'
                    ,'{carInfo["ownerChangedCount"]}'
                    ,'{carInfo["accidentList"]}'
                    ,NOW()
                    ,'1'
                    ,'{carInfo["siteCode"]}'
                )
            '''
            cursor.execute(insertQuery)
            dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴        
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))        
        traceback.print_exc()  # 전체 스택 추적 출력
        setLogPrint("error:carId:["+str({carInfo["carId"]})+"] --- updateQuery:"+str(insertQuery))
        pass

def setCarInfoBiddingCloseUpdate(prdSeq,carId, siteCode,  curSor):
    
    updateQuery =""
    try:
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET
                 BIDDING_STATUS = '경매 종료'
                ,MOD_DATE = NOW()
            WHERE STATUS = '1'
            AND SITE_CODE = '{siteCode}'
            AND PRD_SEQ = '{prdSeq}' 
        '''        
        curSor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("UPDATE TBL_CAR_AUCTION_LIST = CAR_ID:"+str(carId)+"  |  PRD_SEQ:"+str(prdSeq)+"  |  BIDDING_STATUS=경매 종료")
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))        
        setLogPrint("error:carId:["+str({"carId"})+"]--------updateQuery:"+str(updateQuery))
        pass

def setCarInfoDbUpdate(carInfo, cursor):
    try:
        updateQuery = ""
        
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET
                CAR_ID_NUMBER = '{carInfo["carIdNumber"]}'
                --  ,ACA_NUMBER = ''
                --  ,ACA_ROUND = ''
                ,DOMESTIC = '{carInfo["domestic"]}'
                ,KIND = '{carInfo["kind"]}'
                ,MAKER = '{carInfo["makerName"]}'
                ,MODEL = '{carInfo["modelName"]}'
                ,MODEL_DETAIL = '{carInfo["modelDetailName"]}'
                ,GRADE = '{carInfo["gradeName"]}'
                ,GRADE_DETAIL = '{carInfo["gradeDetailName"]}'
                ,YEARS = '{carInfo["years"]}'
                -- ,MONTHS = ''
                ,KM = '{carInfo["km"]}'
                ,MISSION = '{carInfo["mission"]}'
                ,COLOR = '{carInfo["color"]}'
                ,FUEL = '{carInfo["fuel"]}'
                ,PLATES_NUMBER = '{carInfo["platesNumber"]}'
                ,CC = '{carInfo["cc"]}'
                ,FULL_NAME = '{carInfo["fullName"]}'
                ,REG_DATE = '{carInfo["regDate"]}'
                ,DIVIDE = ''
                ,MOD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')
                ,MOD_HOUR = DATE_FORMAT(NOW(), '%H')
                ,AP_MODEL_ID = '{carInfo["apModelId"]}'
                -- ,ETC_TEXT = '{carInfo["etcText"]}'
                -- ,CHECK_TEXT = '{carInfo["checkText"]}'
                ,EVAL_TEXT = '{carInfo["evalText"]}'
                ,NEW_PRICE = '{str(carInfo["newPrice"])}'
                ,MAKE_PRICE = '{str(carInfo["makePrice"])}'
                ,FIRST_DATE = '{carInfo["firstDate"]}'
                -- ,EVAL_GRADE = ''
                ,COLOR_API = '{carInfo["colorApi"]}'
                -- ,ACA_ROUND_DATE = ''
                -- ,START_PRICE = ''
                ,BIDDING_PRICE  = '{str(carInfo["biddingPrice"])}'
                ,BIDDING_STATUS = '{carInfo["biddingStatus"]}'                
                ,MAX_BIDDING_COUNT = '{carInfo["maxBiddingCount"]}'
                ,BIDDING_COUNT = '{carInfo["biddingCount"]}'
                ,MOD_DATE = NOW()
                ,PRD_SUB_SEQ = '{carInfo["prdSubSeq"]}'
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID = '{carInfo["carId"]}' 
        '''        
        cursor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("UPDATE TBL_CAR_AUCTION_LIST = carId:"+str(carInfo["carId"])+"  |  prdSubSeq:"+str(carInfo["prdSubSeq"])+"  |  biddingPrice:"+str(carInfo["biddingPrice"])+"  | biddingStatus:"+str(carInfo["biddingStatus"]))
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))    
        traceback.print_exc()  # 전체 스택 추적 출력    
        setLogPrint("error:carId:["+str({carInfo["carId"]})+"]--------updateQuery:"+str(updateQuery))
        pass
# ────────────────────────────────────────────────────────────────────────────────────────────
# 매입시세에서 오차범위가 낮은 차량의 가격,년식, 주행거리 리턴함
def getLowErrorCarInfo(connInfoFn, carId, carYear, carKm,biddingEndDate):
    
    #print("SOURCE----carId:",carId,"|years:",carYear,"|km:",carKm)
    lowErrorCarInfo = {}
    query = f'''
        SELECT 
            PRD_SUB_SEQ
            ,BIDDING_PRICE 
            ,BIDDING_COUNT 
            ,FUEL 
            ,YEARS 
            ,KM 
        FROM TBL_CAR_AUCTION_SUB_LIST
        WHERE SITE_CODE = '3'
        AND CAR_ID = '{carId}'  
        ORDER BY YEARS ASC , KM ASC        
        '''
    connInfoFn = dbConn.cursor(pymysql.cursors.DictCursor)
    connInfoFn.execute(query)  #쿼리 실행
    result=connInfoFn.fetchall()
    
    # 최소 오차 계산
    min_error = float('inf')
    min_km_error = float('inf')
    best_index = -1

    CAR_YEAR = int(carYear)
    CAR_KM = int(carKm)

    lowErrorKmList = []
    lowErrorYearsList = []    
    lowErrorPrdSubSeq = []    
    lowErrorBidCountList = []
    lowErrorBidPriceList = []
    lowErrorFuelList = [] 
    
    for i,row in enumerate(result):  
        #{i}  , {row['컬럼명']}
        error = abs(int(row['YEARS']) - CAR_YEAR)
        
        if error < min_error:
            min_error = error
            lowErrorKmList = [row['KM']]
            lowErrorYearsList = [row['YEARS']]
            lowErrorPrdSubSeq = [row['PRD_SUB_SEQ']]
            lowErrorBidCountList = [row['BIDDING_COUNT']]
            lowErrorBidPriceList = [row['BIDDING_PRICE']]
            lowErrorFuelList = [row['FUEL']]
            #print("TARGET[LOW]----carId:",carId,"|years:",[row['YEARS']],"|km:",[row['KM']])
        elif error == min_error:        
            lowErrorKmList.append(str(row['KM']))
            lowErrorYearsList.append(str(row['YEARS']))
            lowErrorPrdSubSeq.append(str(row['PRD_SUB_SEQ']))
            lowErrorBidCountList.append(str(row['BIDDING_COUNT']))
            lowErrorBidPriceList.append(str(row['BIDDING_PRICE']))
            lowErrorFuelList.append(str(row['FUEL']))
            #print("TARGET[SAME]----carId:",carId,"|years:",[row['YEARS']],"|km:",[row['KM']])
    
    #print(str(lowErrorKmList))
    for i,row in enumerate(lowErrorKmList):    
        km_error = abs(int(row)-CAR_KM)
        if km_error < min_km_error:
            min_km_error = km_error
            best_index = i
            lowErrorCarInfo["carid"]=carId
            lowErrorCarInfo["prdSubSeq"]=lowErrorPrdSubSeq[best_index]
            lowErrorCarInfo["biddingCount"]=lowErrorBidCountList[best_index]
            lowErrorCarInfo["biddingPrice"]=lowErrorBidPriceList[best_index]
            lowErrorCarInfo["fuel"]=lowErrorFuelList[best_index]
            lowErrorCarInfo["year"]=lowErrorYearsList[best_index]
            lowErrorCarInfo["km"]=lowErrorKmList[best_index]
    if result :
        setLogPrint("오차범위 적은 CAR_ID="+str(carId)+"  | YEAR(SORC:DEST)= "+str(CAR_YEAR)+" : "+str(lowErrorCarInfo["year"])+" | KM(SORC:DEST)= "+str(CAR_KM)+" : "+str(lowErrorCarInfo["km"])+" | BIDDING_END_DATE= "+str(biddingEndDate))
    else:
        setLogPrint("오차범위 적은 CAR_ID="+str(carId)+"  | NOT FOUND -- Low Error Car List  | biddingStatus: 시세 없음")        
    return lowErrorCarInfo
# ────────────────────────────────────────────────────────────────────────────────────────────
# 오토비긴즈(카마트) 연동

def getCarSpecInfoAPI(carNumber):
    carInfoSpec = {}
    carInfoSpec["domestic"]=""
    carInfoSpec["brand"]=""
    carInfoSpec["model"]=""
    carInfoSpec["modelDetail"]=""
    carInfoSpec["grade"]=""
    carInfoSpec["gradeDetail"]=""
    carInfoSpec["fuel"]=""
    carInfoSpec["color"]=""
    carInfoSpec["brand"]=""
    return carInfoSpec   

# ────────────────────────────────────────────────────────────────────────────────────────────
#웹드라이버 로그인 초기화
def getWebDriverLoginProc(driver, loginDivide):
    global VAR_WD_DEFAULT_COUNT
    global VAR_PAGE_NUM

    # loginDivide = 1 > first Login
    # loginDivide = 3 > relogin
    VAR_WD_DEFAULT_COUNT+=1

    if loginDivide == 1:
        setLogPrint("WebDriver ["+str(VAR_WD_DEFAULT_COUNT)+"] 첫번째 START")
        driver = getProxyWebdriverInfo()
        wait = WebDriverWait(driver, 15)

        driver.get(LOGIN_URL)
        setLogPrint("### LOGIN_URL:"+LOGIN_URL)

        #2.2.로그인 입력
        wait.until(EC.presence_of_element_located((By.NAME, "username"))).send_keys(LOGIN_ID)
        driver.find_element(By.NAME, "password").send_keys(LOGIN_PW)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        #2.3.로그인 성공 확인
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '로그아웃')]")))
        setLogPrint("[✓]최초 로그인 처리 성공 ++++")
        secNumber = random.randint(5, 13)
        setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    else:
        driver.quit()
        setLogPrint("WebDriver ["+str(VAR_WD_DEFAULT_COUNT)+"]번쩨 초기화 START")
        driver = getProxyWebdriverInfo()
        wait = WebDriverWait(driver, 15)

        driver.get(LOGIN_URL)
        setLogPrint("### LOGIN_URL:"+LOGIN_URL)

        #2.2.로그인 입력
        wait.until(EC.presence_of_element_located((By.NAME, "username"))).send_keys(LOGIN_ID)
        driver.find_element(By.NAME, "password").send_keys(LOGIN_PW)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        #2.3.로그인 성공 확인
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '로그아웃')]")))
        setLogPrint("[✓]["+str(VAR_WD_DEFAULT_COUNT)+"]번째 로그인 처리 성공 ++++")
        secNumber = random.randint(5, 13)
        setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    return driver


def getCarInfoData(driver, carInfo):
    
    carMartUrl = CARINFO_SYNC_URL
    carMartUrlTemp = carMartUrl.replace("CAR_PLATE_NUMBER", carInfo["platesNumber"]);
    carMartUrlTemp = carMartUrlTemp.replace("VI_NUMBER", "");
    setLogPrint("### carMartUrlTemp:"+carMartUrlTemp)
    
    driver.get(carMartUrlTemp)
    retHtml = driver.page_source        
    driver.implicitly_wait(1)   
    retHtml = retHtml.replace("<html><head></head><body>","").replace("</body></html>","")
    
    carInfoJson = json.loads(retHtml)
            
    if carInfoJson["resultCode"] == "00":
        carInfo["colorApi"] = carInfoJson["resultData"]["color"]                     #카마트기준 색상
        carInfo["newPrice"] = carInfoJson["resultData"]["newPrice"]                  #카마트기준 신차가
        carInfo["makePrice"] = carInfoJson["resultData"]["carMakePrice"]             #카마트기준 출고가        
        carInfo["apModelId"] = carInfoJson["resultData"]["apModelId"]                #카마트기준 모델아이디
        carInfo["carIdNumber"] = carInfoJson["resultData"]["vinCode"]                #카마트기준 차대번호
        carInfo["kindName"] = carInfoJson["resultData"]["kindName"]                  #카마트기준 차종명
        carInfo["domestic"] = carInfoJson["resultData"]["carDomestic"]               #카마트기준 국산/수입 구분  
        carInfo["makerName"] = carInfoJson["resultData"]["makerName"]                #카마트기준 제조사         
        carInfo["modelName"] = carInfoJson["resultData"]["modelName"]                #카마트기준 모델명
        carInfo["modelDetailName"] = carInfoJson["resultData"]["modelDetailName"]    #카마트기준 세부 모델명
        carInfo["gradeName"]  = carInfoJson["resultData"]["gradeName"]               #카마트기준 등급명
        carInfo["gradeDetailName"]  = carInfoJson["resultData"]["gradeDetailName"]   #카마트기준 세부 등급명
        setLogPrint("O CAR-SPEC O Car SpecInfo Find It")
    else:      
        setLogPrint("X CAR-SPEC X Car SpecInfo Not Found CarSpec")
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
    setLogPrint("getProxyWebdriverInfo START")
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
    setLogPrint("getProxyWebdriverInfo END")
    secNumber = random.randint(5, 13)
    setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
    time.sleep(secNumber)
    return driver

#2.타겟 사이트 접속
try:
    driver = getWebDriverLoginProc("",1)
    setLogPrint("Login Complete > Crawlling Start")

    #DB에서 상세화면 크롤링 목록 가져오기
    dbConn = getDbConnectInfo()
    dbCursor = dbConn.cursor()
    setLogPrint("DB Connect Complete")

    #사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
    #BIDDING_END_DATE 가 지난 건은 조회 불가능
    
    query = '''
        UPDATE TBL_CAR_AUCTION_LIST
        SET BIDDING_STATUS ='경매 종료'
            ,SYNC_STATUS ='3'
        WHERE BIDDING_END_DATE <=  DATE_FORMAT(NOW(),'%Y-%m-%d' )
    '''

    dbCursor.execute(query)
    dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴

    query = '''
        SELECT 
            PRD_SEQ
            ,CAR_ID
            ,DETAIL_URL 
            ,BIDDING_END_DATE
        FROM TBL_CAR_AUCTION_LIST
        WHERE SITE_CODE = '3'
        AND SYNC_STATUS ='1'        
        AND BIDDING_END_DATE  > DATE_FORMAT(NOW(),'%Y-%m-%d' )
        ORDER BY REG_DATE ASC
    '''

    connInfo = dbConn.cursor(pymysql.cursors.DictCursor)
    connInfo.execute(query)  #쿼리 실행
    result=connInfo.fetchall()
    
    df = pd.DataFrame(result)

    setLogPrint("Target CarList Select complete")


    subCarListCount = 0
    
    if len(df) > 0:

        procCount = 0
        defaultCount = 0
        dbReConnectCount = 0
        loginCount = 0

        for idx, row in df.iterrows():
            # dict 형태라 키로 접근 가능
            #print(f"[{idx}] {row['PRD_SEQ']} | {row['CAR_ID']} | {row['DETAIL_URL']}")
            prdSeq = row['PRD_SEQ']
            detailUrl = row['DETAIL_URL']
            carId = row['CAR_ID']
            biddingEndDate = row['BIDDING_END_DATE']

            updateQuery = f'''
                    UPDATE TBL_CAR_AUCTION_LIST
                    SET  SYNC_STATUS ='3'
                        ,SITE_CODE='3'
                    WHERE PRD_SEQ = {prdSeq}
                '''
            dbCursor.execute(updateQuery)
            dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴

            setLogPrint("# Seelct >>> PROC >>> carId:["+str(carId)+"]")

            #2.1.로그인 페이지 접속
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
                
            procCount+=1

            targetUrl = TARGET_SITE_DETAIL_URL.replace("VAR_HASH_ID",carId)      
            getListData = getRemoveHtmlTags(getWebSpiderData(driver,targetUrl))
            
            secNumber = random.randint(3, 6) 
            setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
            time.sleep(secNumber)   
            carList = json.loads(getListData)
            
            #print("--------------------------------------------carList------------------------------------------------")
            #setLogPrint("@@@@"+str(carList))
         
            if len(carList) > 5 :
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

                carInfo["kind"] = carhistoryInfo.get("car_type")

                carInfo["cc"] = carhistoryInfo.get("displacement")
                carInfo["model"] = carhistoryInfo.get("model_group")
                
                carInfo["myCarAccidentCount"] = carhistoryInfo.get("my_car_accident_count")
                carInfo["otherCarAccidentCount"] = carhistoryInfo.get("other_car_accident_count")
                carInfo["myCarAccidentCost"] = carhistoryInfo.get("my_car_accident_cost")
                carInfo["otherCarAccidentCost"] = carhistoryInfo.get("other_car_accident_cost")               
                
                carInfo["platesNumber"]=detail.get("car_number")      
                carInfo["carId"]=carList.get("hash_id")
                carInfo["detailHashId"]=detail.get("detail_hash_id")
                carInfo["modelHashId"]=detail.get("model_hash_id")
                carInfo["siteCode"]=VAR_SITE_CODE  # 1: glovice, 3: HeyDealer zero 서비스
                carInfo["domestic"]=""
                carInfo["conditions"]=""
                carInfo["checkText"]=checkText.replace("'","''")
                carInfo["etcText"]=etcText.replace("'","''")
                carInfo["evalText"]=evalText.replace("'","''")
                carInfo["trust"]=""
                carInfo["makerName"]=detail.get("brand_name")
                carInfo["modelName"]=""
                carInfo["modelDetailName"]=detail.get("model_part_name")
                carInfo["gradeName"]=""
                carInfo["gradeDetailName"]=detail.get("grade_part_name")
                carInfo["fullName"]=detail.get("full_name")
                carInfo["mission"]=detail.get("transmission_display")
                carInfo["fuel"]=detail.get("fuel_display")
                carInfo["years"]=str(detail.get('year'))
                carInfo["months"]=""
                carInfo["fromYear"]=""
                carInfo["km"]=str(detail.get('mileage'))
                carInfo["price"]=""
                carInfo["addArea"]=detail.get('short_location_first_part_name')
                carInfo["ModifiedDate"]=""
                carInfo["LeaseType"]=""
                carInfo["color"]=detail.get('color')

                carInfo["firstDate"]=detail.get('initial_registration_date')[0:10]
                carInfo["regDate"]=auction.get("approved_at")[0:10]
                carInfo["biddingStatus"]=carList.get("status_display")
                carInfo["status"]=carList.get("status")
                carInfo["maxBiddingCount"]=str(auction.get("max_bids_count"))
                carInfo["biddingCount"]=str(auction.get("bids_count"))            
                carInfo["newPrice"]=detail.get('standard_new_car_price')
                
                carInfo["makePrice"]=""
                carInfo["apModelId"]=""
                carInfo["colorApi"]=""
                carInfo["kindName"]=""

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

                    subPageNum = 1
                    while True:                        
                        targetSubUrl = TARGET_SITE_PRICE_URL.replace("VAR_PAGE_NUM",str(subPageNum))   
                        targetSubUrl = targetSubUrl.replace("VAR_MODEL_ID",etcData.get("model")) 
                        targetSubUrl = targetSubUrl.replace("VAR_GRADE_ID",etcData.get("grade")) 
                        targetSubUrl = targetSubUrl.replace("VAR_YEAR1",str(years[0]))
                        targetSubUrl = targetSubUrl.replace("VAR_YEAR2",str(years[1]))
                        targetSubUrl = targetSubUrl.replace("VAR_YEAR3",str(years[2]))
                        
                        if str(etcData.get("min_mileage")) is not None:
                            targetSubUrl = targetSubUrl.replace("VAR_MIN_KM",str(etcData.get("min_mileage")))
                        else:
                            targetSubUrl = targetSubUrl.replace("VAR_MIN_KM","")
    
                        if str(etcData.get("max_mileage")) is not None:
                            targetSubUrl = targetSubUrl.replace("VAR_MAX_KM",str(etcData.get("max_mileage")))
                        else:
                            targetSubUrl = targetSubUrl.replace("VAR_MAX_KM","")
                        
                        targetSubUrl = targetSubUrl.replace("None","")                  
                        
                        getPriceListData = getRemoveHtmlTags(getWebSpiderData(driver,targetSubUrl))
                        subCarList = json.loads(getPriceListData)
                        #print("--------------------------------------------subCarList------------------------------------------------")
                        #print("@@@@"+str(subCarList))
                        #print("@@@@@@@@@@@@@@@@@@",len(subCarList))
                        if len(subCarList) > 0 :                            
                            subCarListCount+=1
                            for subCar in subCarList:                                  
                                detailSub = subCar.get("detail", {})
                                auctionSub = subCar.get("auction", {})                                                 
                              
                                subCarInfo = {}
                                subCarInfo["carId"]=carId
                                subCarInfo["modelId"]=etcData.get("model")
                                subCarInfo["gradeId"]=etcData.get("grade")
                                subCarInfo["biddingPrice"]=auctionSub.get("highest_bid", {}).get("price")
                                subCarInfo["biddingCount"]=auctionSub.get("bids_count")
                                subCarInfo["fuel"]=detailSub.get("fuel_display")
                                subCarInfo["gradeDetail"]=detailSub.get("grade_part_name")
                                subCarInfo["mission"]=detailSub.get("transmission_display")
                                subCarInfo["years"]=detailSub.get("year")
                                subCarInfo["km"]=detailSub.get("mileage")
                                subCarInfo["fromEndDate"]=auctionSub.get("ended_at_display")                                
                                accidentArray = detailSub.get("carhistory", {}).get("my_car_accident_summary").split('·')                                
                                subCarInfo["myAccidentPrice"]=accidentArray[1]
                                subCarInfo["myAccidentCount"]=accidentArray[0]
                                subCarInfo["ownerChangedCount"]=detailSub.get("carhistory", {}).get("owner_changed_count")
                                subCarInfo["accidentList"]=str(detailSub.get("accident_repairs", {})).replace("'","''")
                                subCarInfo["siteCode"]=carInfo["siteCode"]                                
                                setAuctionSubInsert(subCarInfo,dbCursor )
                            subPageNum+=1
                        else:                            
                            break                        

                #오차범위 적은 차량 추출
                if subCarListCount > 0 :
                    setLogPrint("++ subCarList Find It List[carId:"+carId+"] > targetSubUrl:"+targetSubUrl)
                    lowErrorCarRetData = {}
                    lowErrorCarRetData = getLowErrorCarInfo(connInfo, carId, carInfo["years"], carInfo["km"],biddingEndDate)
                    if lowErrorCarRetData :
                        setLogPrint("++ subCarList Find It LowErrorCar[carId:"+carId+"]")
                        carInfo["prdSubSeq"]=lowErrorCarRetData["prdSubSeq"]
                        carInfo["biddingPrice"]=lowErrorCarRetData["biddingPrice"]
                        carInfo["biddingStatus"] = "경매 종료"  
                    else:
                        setLogPrint("-- subCarList Not Found LowErrorCar[carId:"+carId+"]")
                        carInfo["prdSubSeq"]="0"
                        carInfo["biddingPrice"]="0"
                        carInfo["biddingStatus"] = "시세 없음"
                else:
                    setLogPrint("-- subCarList Not Found List[carId:"+carId+"] > targetSubUrl:"+targetSubUrl)
                    carInfo["prdSubSeq"]="0"
                    carInfo["biddingPrice"]="0"
                    carInfo["biddingStatus"] = "시세 없음"
                dbConn.commit()                        
                #setLogPrint("###CarInfo:"+str(carInfo))
                
                #Car Info Sync(carmart or autobegins)
                
                carInfo = getCarInfoData(driver, carInfo)
                                
                setCarInfoDbUpdate(carInfo,dbCursor)
                #print("insert: carId:",carInfo["carId"])
                dbConn.commit()   
                secNumber = random.randint(2, 5) 
                setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
                time.sleep(secNumber)  
            else:
                setLogPrint("Notfound Web(VIP) Data[carId:"+str(carId)+"]BiddingEndDate:"+str(biddingEndDate))
                setCarInfoBiddingCloseUpdate(prdSeq,carId, VAR_SITE_CODE, dbCursor)
    else:
        setLogPrint("Notfound Select DB Data")
finally:
    connInfo.close() ## 연결 정도 자원 반환
    dbConn.close()
    
    df.head()
    if not df.empty:                
        df.describe()

    setLogPrint("END")
    
END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("######## END 소요시간:"+str(gapTime)+"분 ############")
