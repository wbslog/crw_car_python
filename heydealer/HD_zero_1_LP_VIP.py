'''
*@author: DoYeon.Shin(JinRoh)
*@date : 2025.10.14
*@desc : Heydealer Auction ListPage crawling (경매건 목록 수집)

* 상태값 정의

'''

import os
import zipfile
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
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import varList

# ────────────────────────────────────────────────────────────────────────────────────────────
BATCH_NAME = "BATCH1_LP_VIP"
START_TIME = datetime.now()

# 1.URL-Info, DB-Info
TARGET_SITE_LP_URL=varList.TARGET_SITE_LP_URL
TARGET_SITE_DETAIL_URL=varList.TARGET_SITE_DETAIL_URL
TARGET_SITE_PRICE_URL=varList.TARGET_SITE_PRICE_URL
USER_AGENT_NAME =varList.USER_AGENT_NAME
CARINFO_SYNC_URL = varList.CARINFO_SYNC_URL

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
# ────────────────────────────────────────────────────────────────────────────────────────────
VAR_PAGE_NUM = 1
VAR_PAGE_BLOCK = 0
VAR_WD_DEFAULT_COUNT = 0
VAR_DB_DEFAULT_COUNT = 0
VAR_INSERT_COUNT = 0
VAR_UPDATE_COUNT = 0
VAR_PROC_COUNT = 0

VAR_SITE_CODE = "3"


# ────────────────────────────────────────────────────────────────────────────────────────────
4.# Proxy IP/PORT get
#사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
def getProxyIpOne():      
    setLogPrint("#getProxyIpOne exec") 
    #CAR_DB서버 접속 정보
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
    
    setLogPrint("get ProxyInfo [IP:"+proxyInfo["PROXY_IP"]+"][PORT:"+proxyInfo["PROXY_PORT"]+"]")
    
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
        setLogPrint("error:url:"+url)
        setLogPrint("error:Exception:{e}")
        setLogPrint("error:retHtml:"+str(retHtml))
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
 
def drop_keys(obj, keys_to_drop: set):
    if isinstance(obj, dict):
        return {k: drop_keys(v, keys_to_drop)
                for k, v in obj.items() if k not in keys_to_drop}
    if isinstance(obj, list):
        return [drop_keys(x, keys_to_drop) for x in obj]
    return obj

def setCarInfoDbInsert(carInfo, cursor):  
    try:
        query = f''' 
            SELECT 
                COUNT(*) AS CNT 
            FROM TBL_CAR_AUCTION_LIST 
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID ='{carInfo["carId"]}'
           
        '''
        #AND ADD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')  제거함
        #print("###query::",query)
        cursor.execute(query)
        result = cursor.fetchall()
        insertQuery = ""
        updateQuery = ""    

        # 1st LP
        if int(result[0]["CNT"]) == 0:
            insertQuery = f'''            
            INSERT INTO TBL_CAR_AUCTION_LIST (
                SITE_CODE
                ,CAR_ID
                ,MODEL_DETAIL_ORI
                ,GRADE_DETAIL_ORI
                ,YEARS
                ,KM
                ,STATUS
                ,ADD_DATE
                ,ADD_YMD
                ,ADD_HOUR
                ,FULL_NAME
                ,ADD_AREA
                ,ETC_TEXT
                ,CHECK_TEXT
                ,FIRST_DATE
                ,BIDDING_STATUS
                ,DETAIL_URL
                ,MAX_BIDDING_COUNT
                ,BIDDING_COUNT
                ,REG_DATE
                ,BRAND_IMAGE_URL
                ,MAIN_IMAGE_URL
                ,BIDDING_END_DATE
                ,SYNC_STATUS
                ,SYNC_TEXT
            )VALUES(		
                '{carInfo["siteCode"]}'
                ,'{carInfo["carId"]}'             
                ,'{carInfo["modelDetailOri"]}'
                ,'{carInfo["gradeDetailOri"]}'
                ,'{carInfo["years"]}'
                ,'{carInfo["km"]}'
                ,'1'
                ,NOW()
                ,DATE_FORMAT(NOW(), '%Y%m%d')
                ,DATE_FORMAT(NOW(), '%H')
                ,'{carInfo["fullName"]}'
                ,'{carInfo["addArea"]}'                
                ,'{carInfo["etcText"]}'
                ,'{carInfo["checkText"]}'
                ,'{carInfo["firstDate"]}'
                ,'{carInfo["biddingStatus"]}'
                ,'{carInfo["detailUrl"]}'
                ,'{carInfo["maxBiddingCount"]}'
                ,'{carInfo["biddingCount"]}'
                ,'{carInfo["regDate"]}'
                ,'{carInfo["brandImageUrl"]}'
                ,'{carInfo["mainImageUrl"]}'               
                ,'{carInfo["biddingEndDate"]}'
                ,'1'
                ,'{carInfo["syncText"]}'
            )
            '''
            cursor.execute(insertQuery)            
            setLogPrint("INFO: LP >>>> INSERT = carId:"+carInfo["carId"]+"/modelDetailOri:"+carInfo["modelDetailOri"]+"/biddingStatus:"+carInfo["biddingStatus"])          
        else:
            # 1st LP
            updateQuery = f'''
                UPDATE TBL_CAR_AUCTION_LIST    
                SET	
                    MODEL_DETAIL_ORI = '{carInfo["modelDetailOri"]}'
                    ,GRADE_DETAIL_ORI = '{carInfo["gradeDetailOri"]}'
                    ,YEARS = '{carInfo["years"]}'
                    ,KM = '{carInfo["km"]}'         
                    ,FULL_NAME = '{carInfo["fullName"]}'
                    ,REG_DATE = '{carInfo["regDate"]}'                   
                    ,MOD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')
                    ,MOD_HOUR = DATE_FORMAT(NOW(), '%H')                  
                    ,ETC_TEXT = '{carInfo["etcText"]}'
                    ,CHECK_TEXT = '{carInfo["checkText"]}'                   
                    ,FIRST_DATE = '{carInfo["firstDate"]}'                  
                    ,BIDDING_STATUS = '{carInfo["biddingStatus"]}'
                    ,DETAIL_URL = '{carInfo["detailUrl"]}'
                    ,MAX_BIDDING_COUNT = '{carInfo["maxBiddingCount"]}'
                    ,BIDDING_COUNT = '{carInfo["biddingCount"]}'
                    ,BRAND_IMAGE_URL = '{carInfo["brandImageUrl"]}'
                    ,MAIN_IMAGE_URL = '{carInfo["mainImageUrl"]}'
                    ,MOD_DATE = NOW()		
                    ,BIDDING_END_DATE = '{carInfo["biddingEndDate"]}'
                WHERE STATUS = '1'
                AND SITE_CODE = '{carInfo["siteCode"]}'
                AND CAR_ID = '{carInfo["carId"]}' 
            '''            
            cursor.execute(updateQuery)   
            setLogPrint("INFO: LP <<<< UPDATE = carId:"+carInfo["carId"]+"/modelDetailOri:"+carInfo["modelDetailOri"]+"/biddingStatus:"+carInfo["biddingStatus"])           
    except Exception as e:
        setLogPrint("Exception Type:"+str(type(e).__name__))
        setLogPrint("Exception Message:"+str(e))
        setLogPrint("error:carId:"+carInfo["carId"])
        setLogPrint("error:insertQuery:"+str(insertQuery))
        setLogPrint("error:updateQuery:"+str(updateQuery))
        pass
    

def setCarInfoDbUpdate(carInfo, cursor):
    try:
        updateQuery = ""
        
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET
                PLATE_NUMBER = '{carInfo["plateNumber"]}'
                --  ,VIN_NUMBER = ''
                --  ,ACA_NUMBER = ''
                --  ,ACA_ROUND = ''
                ,DOMESTIC = '{carInfo["domestic"]}'
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
                ,CC = '{carInfo["cc"]}'                
                ,FULL_NAME = '{carInfo["fullName"]}'
                ,REG_DATE = '{carInfo["regDate"]}' 
                ,AP_MODEL_ID = '{carInfo["apModelId"]}'
                -- ,ETC_TEXT = '{carInfo["etcText"]}'
                -- ,CHECK_TEXT = '{carInfo["checkText"]}'
                ,EVAL_TEXT = '{carInfo["evalText"]}'
                ,NEW_PRICE = '{str(carInfo["newPrice"])}'
                ,MAKE_PRICE = '{str(carInfo["makePrice"])}'              
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
                ,CAR_INFO_SYNC = '{carInfo["carInfoSync"]}'
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID = '{carInfo["carId"]}' 
            AND SYNC_STATUS = '1'
        '''        
        cursor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("INFO: <<< CARMART UPDATE TBL_CAR_AUCTION_LIST = carId: "+str(carInfo["carId"])+" | modelId: | "+str(carInfo["apModelId"])+" | biddingPrice:"+str(carInfo["biddingPrice"])+" | biddingStatus:"+str(carInfo["biddingStatus"]))
    except Exception as e:
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))    
        traceback.print_exc()  # 전체 스택 추적 출력    
        setLogPrint("ERROR: error:carId: ["+str({carInfo["carId"]})+"]--------updateQuery:"+str(updateQuery))
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

#웹드라이버 로그인 초기화
def getWebDriverLoginProc(driver, loginDivide): 
       
    global VAR_WD_DEFAULT_COUNT
    global VAR_PAGE_NUM

    # loginDivide = 1 > first Login
    # loginDivide = 3 > relogin
    VAR_WD_DEFAULT_COUNT+=1   
            
    if loginDivide == 1 :                   
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
        secNumber = random.randint(10, 20)
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
        secNumber = random.randint(5, 20)
        setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)   

    return driver



# 오토비긴즈(카마트) 연동 -------------------------------------------------------------------------
def getCarSpecInfoAPI(driver, carInfo, cursor):
    
    carInfoJson = {}
    
    # DB에서 해당 modelId, gradeId 로 조회해서 ap_model_id가 있을경우 해당 Ap_model_id, model, model_detail, grade, grade_detail, years, color, mission 을 가져온다.    
    query = f''' 
        SELECT 
            AP_MODEL_ID
            ,KIND
            ,MAKER
            ,DOMESTIC
            ,MODEL
            ,MODEL_DETAIL
            ,GRADE
            ,GRADE_DETAIL
            ,MISSION
            ,COLOR
            ,FUEL
            ,NEW_PRICE
            ,MAKE_PRICE
        FROM TBL_CAR_AUCTION_LIST 
        WHERE STATUS = '1'
        AND SITE_CODE = '{carInfo["siteCode"]}'
        AND MODEL_CODE1 ='{carInfo["modelId"]}'
        AND MODEL_CODE2 = '{carInfo["gradeId"]}'
        AND GRADE_DETAIL_ORI = '{carInfo["gradeDetailOri"]}'    
        ORDER BY CAST(AP_MODEL_ID AS UNSIGNED ) DESC
        LIMIT 1
    '''
    #AND ADD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')  제거함
    #print("###query::",query)
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)

    if len(df) > 0 :    
        carInfo["apModelId"] = df["AP_MODEL_ID"][0]         #카마트기준 모델아이디
        carInfo["color"] = df["COLOR"][0]                   #카마트기준 색상
        if not len(str(carInfo["newPrice"])) > 1 :
            carInfo["newPrice"] = df["NEW_PRICE"][0]        #카마트기준 신차가        
        
        carInfo["makePrice"] =  df["MAKE_PRICE"][0]         #카마트기준 출고가        
        carInfo["vinNumber"] = ""                           #카마트기준 차대번호
        carInfo["kind"] = df["KIND"][0]                     #카마트기준 차종명
        carInfo["domestic"] = df["DOMESTIC"][0]             #카마트기준 국산/수입 구분  
        carInfo["maker"] = df["MAKER"][0]                   #카마트기준 제조사         
        carInfo["model"] = df["MODEL"][0]                   #카마트기준 모델명
        carInfo["modelDetail"] = df["MODEL_DETAIL"][0]      #카마트기준 세부 모델명
        carInfo["grade"]  = df["GRADE"][0]                  #카마트기준 등급명
        carInfo["gradeDetail"]  = df["GRADE_DETAIL"][0]     #카마트기준 세부 등급명
        carInfo["years"]  = ""                              #카마트기준 년식
        carInfo["fuel"]  = df["FUEL"][0]                    #카마트기준 년식
        carInfo["mission"] = df["MISSION"][0]               #카마트기준 미션(기어)
        carInfo["carInfoSync"] = "internal data"            #카마트기준 미션(기어)
        
        setLogPrint("INFO: >>> CAR-INFO: DB SELECT SUCCESS - AP_MODEL_ID:"+carInfo["apModelId"]+" | GRADE_DETAIL_ORI: "+str({carInfo["gradeDetailOri"]}))
    else:
        try:
            #carInfo["plateNumber"]="230수1789"
            carMartUrl = CARINFO_SYNC_URL
            carMartUrlTemp = carMartUrl.replace("CAR_PLATE_NUMBER", carInfo["plateNumber"]);
            carMartUrlTemp = carMartUrlTemp.replace("VI_NUMBER", "");
            #setLogPrint("LINK: carMartUrlTemp:"+carMartUrlTemp)
            response = requests.get(carMartUrlTemp, timeout=10)            
            retHtml = str(response.text)
            
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
                carInfo["newPrice"] = carInfoJson["resultData"]["newPrice"]                 #카마트기준 모델명
                carInfo["modelDetail"] = carInfoJson["resultData"]["modelDetailName"]       #카마트기준 세부 모델명
                carInfo["grade"]  = carInfoJson["resultData"]["gradeName"]             #카마트기준 신차가
                carInfo["makePrice"] = carInfoJson["resultData"]["carMakePrice"]            #카마트기준 출고가        
                carInfo["vinNumber"] = carInfoJson["resultData"]["vinCode"]                 #카마트기준 차대번호
                carInfo["kind"] = carInfoJson["resultData"]["kindName"]                     #카마트기준 차종명
                carInfo["domestic"] = carInfoJson["resultData"]["carDomestic"]              #카마트기준 국산/수입 구분  
                carInfo["maker"] = carInfoJson["resultData"]["makerName"]                   #카마트기준 제조사         
                carInfo["model"] = carInfoJson["resultData"]["modelName"]                        #카마트기준 등급명
                carInfo["gradeDetail"]  = carInfoJson["resultData"]["gradeDetailName"]      #카마트기준 세부 등급명
                carInfo["years"]  = carInfoJson["resultData"]["carYear"]                      #카마트기준 년식
                carInfo["fuel"]  = carInfoJson["resultData"]["fuel"]                        #카마트기준 년식
                carInfo["mission"] = carInfoJson["resultData"]["gearBox"]                   #카마트기준 미션(기어)
                carInfo["carInfoSync"] = "carinfo-api:findIt"
                setLogPrint("INFO: >>> CAR-INFO CAR-MART SYNC SUCCESS - AP_MODEL_ID:"+carInfo["apModelId"]+" | MODEL: "+carInfo["model"])
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
                carInfo["carInfoSync"] = "carinfo-api:notFound"     
                setLogPrint("WARN: XXX CAR-INFO CAR-MART Not Found Car-Spec")
        except Exception as e:
            print("=" * 50)
            print("전체 Traceback:")
            traceback.print_exc()
            setLogPrint("Exception Type:"+str(type(e).__name__))
            setLogPrint("Exception Message:"+str(e))
            setLogPrint("-------------- CarMart:crawlling Error --------------")
            setLogPrint("######### CAR-MART-ERROR: carMartUrl:"+str(carMartUrlTemp))
            setLogPrint("######### CAR-MART-ERROR: carNumber:["+str(carInfo["plateNumber"])+"] | carInfoJson:"+str(carInfoJson))
        except Timeout:
        # 타임아웃 재시도 로직/로그
            raise
        except HTTPError as e:
            # 상태코드별 처리
            print("HTTPError", e.response.status_code, e.response.text[:200])
            raise
        except ValueError as e:
            # JSON 아님/파싱 실패
            print("JSON 파싱 실패:", str(e))
            # 필요시 r.text로 원문 확인
            # print(r.text[:500])
            raise
        except RequestException as e:
            # 기타 네트워크 예외
            print("요청 실패:", e)
            raise    
    return carInfo


def setCarInfoBiddingCloseUpdate(carId, siteCode,  curSor, syncText):
    
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
            AND CAR_ID = '{carId}'
        '''        
        curSor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("INFO: UPDATE TBL_CAR_AUCTION_LIST = carId: "+str(carId)+"  | SYNC_TEXT="+syncText)
    except Exception as e:
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))        
        setLogPrint("ERROR: error:carId: ["+str({"carId"})+"]-updateQuery:"+str(updateQuery))
        pass

def getDetailRead(driver, dbCursor, carId, biddingEndDate, gradeDetailOri ):
    try:
        targetUrl = TARGET_SITE_DETAIL_URL.replace("VAR_HASH_ID",carId)      
        getListData = getRemoveHtmlTags(getWebSpiderData(driver,targetUrl))
        setLogPrint("LINK: DetailUrl:"+targetUrl)
        
        secNumber = 1
        setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)   
       
        carListSub = json.loads(getListData)
        
        #print("--------------------------------------------carList------------------------------------------------")
        #setLogPrint("@@@@"+str(carList))
        ## 로그인이 풀린경우 중지시킴
        # {"code":null,"message":null,"toast":{"message":"로그인 후 사용해주세요.","type":"default"},"popup":null,"toast_message":"로그인 후 사용해주세요."}
        if "toast_message" in carListSub:
            if carListSub.get("toast_message").find("로그인 후 사용") > -1 :
                setLogPrint("ERROR: Duplicate login detected and stopped")
                exit(0)
            else:
                if carListSub.get("toast_message").find("권한이 없습니다") > -1 :
                    setLogPrint("INFO: XXX 경매건이 존재하지 않음( VIP Not Found Page - carId: "+str(carId)+"]BiddingEndDate:"+str(biddingEndDate))
                    setCarInfoBiddingCloseUpdate(carId, VAR_SITE_CODE, dbCursor, "상세화면 없음(HD-VIP)")
                    setLogPrint("--------------------------------------------------- NEXT1 ---------------------------------------------------")
                else:
                    setLogPrint("INFO: 경매 상세화면 있음 (VIP Find It - carId: "+str(carId) +")" )                   
                    setLogPrint("--------------------------------------------------- NEXT2 ---------------------------------------------------")                             
        else:   
            carInfo = {}
            detail = carListSub.get("detail", {})
            auction = carListSub.get("auction", {})
            etc = carListSub.get("etc", {})

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
            
            carInfo["gradeDetailOri"] = gradeDetailOri
            carInfo["kindOri"] = carhistoryInfo.get("car_type")
            carInfo["cc"] = carhistoryInfo.get("displacement")
            carInfo["modelOri"] = carhistoryInfo.get("model_group")            
            carInfo["myCarAccidentCount"] = carhistoryInfo.get("my_car_accident_count")
            carInfo["otherCarAccidentCount"] = carhistoryInfo.get("other_car_accident_count")
            carInfo["myCarAccidentCost"] = carhistoryInfo.get("my_car_accident_cost")
            carInfo["otherCarAccidentCost"] = carhistoryInfo.get("other_car_accident_cost") 
            carInfo["plateNumber"]=detail.get("car_number")      
            carInfo["carId"]=carListSub.get("hash_id")
            carInfo["detailHashId"]=detail.get("detail_hash_id")
            carInfo["modelHashId"]=detail.get("model_hash_id")
            carInfo["siteCode"]=VAR_SITE_CODE  # 1: glovice, 3: HeyDealer zero 서비스              
            carInfo["checkText"]=checkText.replace("'","''")
            carInfo["etcText"]=etcText.replace("'","''")
            carInfo["evalText"]=evalText.replace("'","''")
            carInfo["makerOri"]=detail.get("brand_name")
            carInfo["fullName"]=detail.get("full_name")
            carInfo["missionOri"]=detail.get("transmission_display")
            carInfo["fuelOri"]=detail.get("fuel_display")            
            carInfo["addArea"]=detail.get('short_location_first_part_name')
            carInfo["colorOri"]=detail.get('color')
            carInfo["firstDate"]=detail.get('initial_registration_date')[0:10]
            carInfo["regDate"]=auction.get("approved_at")[0:10]
            carInfo["biddingStatus"]=carListSub.get("status_display") 
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
                carInfo["year1"] = ""
                carInfo["year2"] = ""
                carInfo["year3"] = ""
                carInfo["minKm"] = ""
                carInfo["maxKm"] = ""
                carInfo["brandId"] = ""
                carInfo["modelGroupId"] = ""
                carInfo["modelId"] = ""
                carInfo["gradeId"] = ""
                carInfo["priceListUrl"] = ""
                            
            carInfo = getCarSpecInfoAPI(driver, carInfo, dbCursor)          
            setCarInfoDbUpdate(carInfo,dbCursor)                
            dbConn.commit()                         
            setLogPrint("INFO: VIP <<<< 상세화면 UPDATE - 차량번호:"+carInfo["plateNumber"]+" | carId: "+carInfo["carId"]+" <<<" )
    except Timeout:
        # 타임아웃 재시도 로직/로그
        driver.quit()
        raise
    except HTTPError as e:
        # 상태코드별 처리
        print("HTTPError", e.response.status_code, e.response.text[:200])
        driver.quit()
        raise
    except ValueError as e:
        # JSON 아님/파싱 실패
        print("JSON 파싱 실패:", str(e))
        # 필요시 r.text로 원문 확인
        # print(r.text[:500])
        driver.quit()
        raise
    except RequestException as e:
        # 기타 네트워크 예외
        print("요청 실패:", e)
        driver.quit()
        raise
                
#2.타겟 사이트 접속
try:
    
    carList = {}
    #2.1.로그인 페이지 접속  
    driver = getWebDriverLoginProc("",1)
    setLogPrint("Login Complete > Crawlling Start") 
     
    i = 0
        
    dbConn = getDbConnectInfo()
    dbCursor = dbConn.cursor()   
    setLogPrint("DB Connect Complete")          

    while True:  
                            
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
        getListData = getRemoveHtmlTags(getWebSpiderData(driver,targetUrl))
    	
        carList = json.loads(getListData)
        if len(carList) > 0 :
            for car in carList:
              
                carInfo = ""
                detail = car.get("detail", {})
                auction = car.get("auction", {})
                tags = auction.get("tags", [])
                
             
                checkText = ""  
                etcText = ""              
                for i,tag in enumerate(tags):                               
                    etcText+=tag.get("text")+"|"
                    if i == 0:
                        checkText = tag.get("text")    

                # 1st LP
                carInfo = {}
                carInfo["carId"]=car.get("hash_id")
                carInfo["siteCode"]="3" # 1: glovice, 3: HeyDealer zero 서비스
                carInfo["domestic"]=""
                carInfo["conditions"]=""
                carInfo["checkText"]=checkText
                carInfo["etcText"]=etcText
                carInfo["trust"]=""
                carInfo["makerOri"]=""
                carInfo["modelOri"]=""
                carInfo["modelDetailOri"]=detail.get("model_part_name")
                carInfo["gradeOri"]=""
                carInfo["gradeDetailOri"]=detail.get("grade_part_name")
                carInfo["fullName"]=str(detail.get("full_name")).replace("'", "''")
                carInfo["missionOri"]=""
                carInfo["fuelOri"]=""
                carInfo["colorOri"]=""
                carInfo["years"]=str(detail.get('year'))
                carInfo["months"]=""
                carInfo["fromYear"]=""
                carInfo["km"]=str(detail.get('mileage'))
                carInfo["price"]=""
                carInfo["addArea"]=detail.get('short_location_first_part_name')
                carInfo["ModifiedDate"]=""
                carInfo["LeaseType"]=""                
                carInfo["firstDate"]=detail.get('initial_registration_date')[0:10]
                carInfo["regDate"]=auction.get("approved_at")[0:10]
                carInfo["biddingStatus"]=car.get("status_display")
                #carInfo["status"]=car.get("status")
                carInfo["maxBiddingCount"]=str(auction.get("max_bids_count"))
                carInfo["biddingCount"]=str(auction.get("bids_count"))
                carInfo["detailUrl"]=TARGET_SITE_DETAIL_URL.replace('VAR_HASH_ID',car.get("hash_id"))                
                carInfo["brandImageUrl"]=detail.get("brand_image_url")
                carInfo["mainImageUrl"]=detail.get("main_image_url")
                carInfo["biddingEndDate"]=auction.get("end_at")[0:10]
                carInfo["syncText"] = "시세수집대기(HD-LP)"
    
                #print("###CarInfo:"+str(carInfo))
                setCarInfoDbInsert(carInfo,dbCursor) 
                
                getDetailRead(driver, dbCursor, car.get("hash_id"), auction.get("end_at")[0:10], carInfo["gradeDetailOri"] )
                #print("insert: carId:",carInfo["carId"])     
                dbConn.commit()
                VAR_PROC_COUNT+=1
                setLogPrint("--------------------------------------------------- PROC COUNT:["+str(VAR_PROC_COUNT)+"]-------------------------------------------------")
        else:
            # EOD(End Of Document)
            driver.quit()
            setLogPrint("End of Documents -> Break")  
            break         
        
        END_TIME = datetime.now()
        TIME_GAP = END_TIME - START_TIME
        elapsed_minutes = TIME_GAP.total_seconds()/60
        gapTime = f"{elapsed_minutes:.2f}"

        setLogPrint("--------------------------------------------------- ["+str(VAR_PAGE_NUM)+"] PAGE Complete >> "+str(gapTime)+"분 소요됨--------------------------------")
        secNumber = random.randint(5, 13) 
        setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    driver.quit()
except Timeout:
    driver.quit()
    # 타임아웃 재시도 로직/로그
    raise
except HTTPError as e:
    # 상태코드별 처리
    driver.quit()
    print("HTTPError", e.response.status_code, e.response.text[:200])
    raise
except ValueError as e:
    # JSON 아님/파싱 실패
    driver.quit()
    print("JSON 파싱 실패:", str(e))
    # 필요시 r.text로 원문 확인
    # print(r.text[:500])
    raise
except RequestException as e:
    # 기타 네트워크 예외
    driver.quit()
    print("요청 실패:", e)
    raise
except Exception as e:
    driver.quit()
    setLogPrint("Exception -------------------------------------------------------- targetUrl:"+targetUrl)
    setLogPrint("Exception -------------------------------------------------------- Html:"+str(getListData))
    print("=" * 50)
    print("전체 Traceback:")
    traceback.print_exc()
    setLogPrint("Exception Type:"+str(type(e).__name__))
    setLogPrint("Exception Message:"+str(e))
    setLogPrint("-------------- Main:crawlling Error --------------")
    pass
finally:
    #드라이버 종료
    dbCursor.close()
    dbConn.commit()
    dbConn.close()
    driver.quit()

    
END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("######## END 소요시간:"+str(gapTime)+"분 ############")
                                        
                                         