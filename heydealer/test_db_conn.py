
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
                LIMIT 10
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

URL="https://findip.kr"
target_str = "내 아이피 주소(IP Address)"

setLogPrint("get proxy web driver START")
driver = getProxyWebdriverInfo()
driver.get(URL)

retHtml = driver.page_source
driver.implicitly_wait(10)  

setLogPrint("get proxy web driver END")

for line in retHtml.split("\n"):
    if target_str in line:
        setLogPrint(line)            


setLogPrint("DB Connect Start")
dbConn = getDbConnectInfo()
dbCursor = dbConn.cursor()


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
        limit 20
    '''

connInfo = dbConn.cursor(pymysql.cursors.DictCursor)
connInfo.execute(query)  #쿼리 실행
result=connInfo.fetchall()
df = pd.DataFrame(result)

setLogPrint("len(df)"+str(len(df)))

if len(df) > 0:  
    for idx, row in df.iterrows():    
        setLogPrint("idx:"+str(idx)+" | carId:"+str(row["CAR_ID"]))
else:
    setLogPrint("Not Found Data")