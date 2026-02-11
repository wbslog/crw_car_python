
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
 

def setMssDbInsert(carInfo, cursor):  
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
    
        if int(result[0]["CNT"]) == 0:
            insertQuery = f'''            
            INSERT INTO TBL_CAR_AUCTION_LIST (
                SITE_CODE
                ,CAR_ID
                ,CAR_ID_NUMBER
                ,ACA_NUMBER
                ,ACA_ROUND
                ,START_PRICE
                ,BIDDING_PRICE
                ,DOMESTIC
                ,KIND
                ,MAKER
                ,MODEL
                ,MODEL_DETAIL
                ,GRADE
                ,GRADE_DETAIL
                ,YEARS
                ,MONTHS
                ,KM
                ,MISSION
                ,COLOR
                ,PRICE
                ,FUEL
                ,PLATES_NUMBER
                ,CC
                ,OPTIONS
                ,STATUS
                ,ADD_DATE			
                ,ADD_YMD
                ,ADD_HOUR
                ,FULL_NAME
                ,CATAL_CODE
                ,ADD_AREA
                ,ACCIDENT
                ,NAVIGATION
                ,SUNROOF
                ,SMARTKEY
                ,LEATHER_SEAT
                ,HEAT_SEAT
                ,VENTI_SEAT
                ,REAL_CAMERA
                ,PARKING_SENSOR
                ,DIVIDE			
                ,MODEL_ID
                ,MODEL_CODE1
                ,MODEL_CODE2
                ,MODEL_CODE3
                ,MODEL_CODE4
                ,AP_MODEL_ID
                ,VIEW_COUNT
                ,FAVORITE_COUNT
                ,CALL_COUNT
                ,FRAME_REPAIR
                ,PANEL_REPAIR
                ,CERTI_STATUS
                ,CERTI_TEXT
                ,CRUISE_CONTROLLER
                ,HEADUP_DISPLAY
                ,ETC_TEXT
                ,CHECK_TEXT
                ,EVAL_TEXT
                ,NEW_PRICE
                ,MAKE_PRICE
                ,FIRST_DATE
                ,BIDDING_STATUS
                ,EVAL_GRADE
                ,COLOR_API
                ,ACA_ROUND_DATE
                ,DETAIL_URL
                ,MAX_BIDDING_COUNT
                ,BIDDING_COUNT
                ,REG_DATE
                ,BRAND_IMAGE_URL
                ,MAIN_IMAGE_URL
                ,SYNC_STATUS
                ,BIDDING_END_DATE
            )VALUES(		
                '{carInfo["siteCode"]}'
                ,'{carInfo["carId"]}'
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,'{carInfo["modelDetail"]}'
                ,''
                ,'{carInfo["gradeDetail"]}'
                ,'{carInfo["years"]}'
                ,''
                ,'{carInfo["km"]}'
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,'1'
                ,NOW()
                ,DATE_FORMAT(NOW(), '%Y%m%d')
                ,DATE_FORMAT(NOW(), '%H')
                ,'{carInfo["fullName"]}'
                ,''
                ,'{carInfo["addArea"]}'
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,'0'
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,''
                ,'{carInfo["etcText"]}'
                ,'{carInfo["checkText"]}'
                ,''
                ,''
                ,''
                ,'{carInfo["firstDate"]}'
                ,'{carInfo["biddingStatus"]}'
                ,''
                ,''
                ,''
                ,'{carInfo["detailUrl"]}'
                ,'{carInfo["maxBiddingCount"]}'
                ,'{carInfo["biddingCount"]}'
                ,'{carInfo["regDate"]}'
                ,'{carInfo["brandImageUrl"]}'
                ,'{carInfo["mainImageUrl"]}'
                ,'1'
                ,'{carInfo["biddingEndDate"]}'
            )
            '''
            cursor.execute(insertQuery)            
            setLogPrint("INSERT = carId:"+carInfo["carId"]+"/modelDetail:"+carInfo["modelDetail"]+"/biddingStatus:"+carInfo["biddingStatus"])
        else:
            updateQuery = f'''
                UPDATE TBL_CAR_AUCTION_LIST    
                SET	                 
                    CAR_ID_NUMBER = ''
                    ,ACA_NUMBER = ''
                    ,ACA_ROUND = ''
                    ,DOMESTIC = ''
                    ,KIND = ''
                    ,MAKER = ''
                    ,MODEL = ''
                    ,MODEL_DETAIL = '{carInfo["modelDetail"]}'
                    ,GRADE = ''
                    ,GRADE_DETAIL = '{carInfo["gradeDetail"]}'
                    ,YEARS = '{carInfo["years"]}'
                    ,MONTHS = ''
                    ,KM = '{carInfo["km"]}'
                    ,MISSION = ''
                    ,COLOR = ''
                    ,FUEL = ''
                    ,PLATES_NUMBER = ''
                    ,CC = ''
                    ,FULL_NAME = '{carInfo["fullName"]}'
                    ,REG_DATE = '{carInfo["regDate"]}'
                    ,DIVIDE = ''
                    ,MOD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')
                    ,MOD_HOUR = DATE_FORMAT(NOW(), '%H')
                    ,AP_MODEL_ID = ''
                    ,ETC_TEXT = '{carInfo["etcText"]}'
                    ,CHECK_TEXT = '{carInfo["checkText"]}'
                    ,EVAL_TEXT = ''
                    ,NEW_PRICE = ''
                    ,MAKE_PRICE = ''
                    ,FIRST_DATE = '{carInfo["firstDate"]}'
                    ,EVAL_GRADE = ''
                    ,COLOR_API = ''
                    ,ACA_ROUND_DATE = ''
                    ,START_PRICE = ''
                    ,BIDDING_PRICE  = ''
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
            setLogPrint("UPDATE = carId:"+carInfo["carId"]+"/modelDetail:"+carInfo["modelDetail"]+"/biddingStatus:"+carInfo["biddingStatus"])
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
    setLogPrint("ChromeDriver Connect Complete")    
    secNumber = random.randint(5, 13) 
    setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
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

#2.타겟 사이트 접속

try:
    
    carList = ""
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

                carInfo = {}
                carInfo["carId"]=car.get("hash_id")
                carInfo["siteCode"]="3" # 1: glovice, 3: HeyDealer zero 서비스
                carInfo["domestic"]=""
                carInfo["conditions"]=""
                carInfo["checkText"]=checkText
                carInfo["etcText"]=etcText
                carInfo["trust"]=""
                carInfo["maker"]=""
                carInfo["model"]=""
                carInfo["modelDetail"]=detail.get("model_part_name")
                carInfo["grade"]=""
                carInfo["gradeDetail"]=detail.get("grade_part_name")
                carInfo["fullName"]=str(detail.get("full_name")).replace("'", "''")
                carInfo["mission"]=""
                carInfo["fuel"]=""
                carInfo["years"]=str(detail.get('year'))
                carInfo["months"]=""
                carInfo["formYear"]=""
                carInfo["km"]=str(detail.get('mileage'))
                carInfo["price"]=""
                carInfo["addArea"]=detail.get('short_location_first_part_name')
                carInfo["ModifiedDate"]=""
                carInfo["LeaseType"]=""
                carInfo["color"]=""
                carInfo["firstDate"]=detail.get('initial_registration_date')[0:10]
                carInfo["regDate"]=auction.get("approved_at")[0:10]
                carInfo["biddingStatus"]=car.get("status_display")
                carInfo["status"]=car.get("status")
                carInfo["maxBiddingCount"]=str(auction.get("max_bids_count"))
                carInfo["biddingCount"]=str(auction.get("bids_count"))
                carInfo["detailUrl"]=TARGET_SITE_DETAIL_URL.replace('VAR_HASH_ID',car.get("hash_id"))                
                carInfo["brandImageUrl"]=detail.get("brand_image_url")
                carInfo["mainImageUrl"]=detail.get("main_image_url")
                carInfo["biddingEndDate"]=auction.get("end_at")[0:10]
    
                #print("###CarInfo:"+str(carInfo))
                setMssDbInsert(carInfo,dbCursor)            
                #print("insert: carId:",carInfo["carId"])     
                dbConn.commit()               
        else:
            # EOD(End Of Document)
            setLogPrint("End of Documents -> Break")  
            break         
        setLogPrint("Now Page:["+str(VAR_PAGE_NUM)+" ] Page Complete")  
        secNumber = random.randint(5, 13) 
        setLogPrint(f">>> time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
except Exception as e:
    setLogPrint("Exception Type:"+str(type(e).__name__))
    setLogPrint("Exception Message:"+str(e))
    setLogPrint("--------------  crawlling Error --------------")
    setLogPrint("######### carList:"+str(carList))
    pass
    
END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("######## END 소요시간:"+str(gapTime)+"분 ############")
                                        
                                         