'''
*@author: DoYeon.Shin(JinRoh)
*@date : 2025.10.14
*@desc : Heydealer Auction PriceList Crawling (시세수집)

*@기능정의
  - 경매시세 조회 대상 차량 목록 DB에서 자겨오기
  - 시세목록 정보 수집
  - 시세 목록에서 경매 차량과 가장 유사도가 높은 차량 추출 및 낙찰가격을 경매 데이터에 업데이트 > 해당 매칭된 시세차량의 PK를 경매건에 업데이트
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
import re
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

def escape_sql_string(value):
    """작은따옴표를 이스케이프"""
    if value is None:
        return 'NULL'
    return "'" + str(value).replace("'", "''") + "'"

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
            AND BIDDING_PRICE = '{carInfo["biddingPrice"]}'         
        '''
        #AND ADD_YMD = DATE_FORMAT(NOW(), '%Y%m%d')  제거함
        #print("###query::",query)
        cursor.execute(query)
        result = cursor.fetchall()

        if int(result[0]["CNT"]) == 0:            
            myAccidentCount = json.dumps(carInfo["myAccidentCount"], ensure_ascii=False)
            accidentList = json.dumps(carInfo["accidentList"], ensure_ascii=False)
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
                    ,'{myAccidentCount}'
                    ,'{carInfo["ownerChangedCount"]}'
                    ,'{accidentList}'
                    ,NOW()
                    ,'1'
                    ,'{carInfo["siteCode"]}'
                )
            '''            
            cursor.execute(insertQuery)    
            return "I"
        else:
            return "D"
    except Exception as e:
        print("---------------insertQuery:",insertQuery)
        print("전체 Traceback:")
        traceback.print_exc()          
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))        
        traceback.print_exc()  # 전체 스택 추적 출력
        setLogPrint("ERROR: carId:["+str({carInfo["carId"]})+"] --- updateQuery:"+str(insertQuery))
        pass

def setCarInfoDbUpdate(carInfo, cursor):
    try:
        updateQuery = ""
        
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET               
                BIDDING_PRICE  = '{str(carInfo["biddingPrice"])}'
                ,BIDDING_STATUS = '{carInfo["biddingStatus"]}'                             
                ,MOD_DATE = NOW()
                ,PRD_SUB_SEQ = '{carInfo["prdSubSeq"]}'
                ,SYNC_STATUS = '{carInfo["syncStatus"]}'
                ,SYNC_TEXT = '{carInfo["syncText"]}'
            WHERE STATUS = '1'
            AND SITE_CODE = '{carInfo["siteCode"]}'
            AND CAR_ID = '{carInfo["carId"]}' 
        '''        
        cursor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("UPDATE TBL_CAR_AUCTION_LIST = carId:"+str(carInfo["carId"])+"  |  prdSubSeq:"+str(carInfo["prdSubSeq"])+"  |  biddingPrice:"+str(carInfo["biddingPrice"])+"  | biddingStatus:"+str(carInfo["biddingStatus"]))
    except Exception as e:
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))    
        traceback.print_exc()  # 전체 스택 추적 출력    
        setLogPrint("ERROR: error:carId:["+str({carInfo["carId"]})+"]--------updateQuery:"+str(updateQuery))
        pass

def setCarInfoBiddingCloseUpdate(prdSeq,carId, siteCode,  curSor, syncText , syncStatus ):
    
    #시세 정보가 존재하지 않음(해당 차량에 연결된 시세가 없음)
    updateQuery =""
    try:
        updateQuery = f'''
            UPDATE TBL_CAR_AUCTION_LIST
            SET
                 BIDDING_STATUS = '경매종료'
                ,MOD_DATE = NOW()
                ,SYNC_STATUS ='{syncStatus}'
                ,SYNC_TEXT = '{syncText}'
            WHERE STATUS = '1'
            AND SITE_CODE = '{siteCode}'
            AND PRD_SEQ = '{prdSeq}' 
        '''        
        curSor.execute(updateQuery)
        dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴
        setLogPrint("INFO: UPDATE TBL_CAR_AUCTION_LIST = carId: "+str(carId)+"  |  PRD_SEQ:"+str(prdSeq)+"  |  BIDDING_STATUS="+syncText)
    except Exception as e:
        setLogPrint("ERROR: Exception Type:"+str(type(e).__name__))
        setLogPrint("ERROR: Exception Message:"+str(e))        
        setLogPrint("ERROR: error:carId: ["+str({"carId"})+"]-updateQuery:"+str(updateQuery))
        pass
# ────────────────────────────────────────────────────────────────────────────────────────────
# 매입시세에서 매칭율이 높은 차량의 가격,년식, 주행거리 리턴함
def getHighMatchingCarInfo(connInfoFn, carId, carYear, carKm,biddingEndDate):
    
    setLogPrint("getHighMatchingCarInfo >> carId: "+str(carId)+" | carYear: "+str(carYear)+" | carKm: "+str(carKm)+" | biddingEndDate: "+str(biddingEndDate))
    
    try:
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
            setLogPrint("INFO: 오차범위 적은 CAR_ID="+str(carId)+"  | YEAR(SORC:DEST)= "+str(CAR_YEAR)+" : "+str(lowErrorCarInfo["year"])+" | KM(SORC:DEST)= "+str(CAR_KM)+" : "+str(lowErrorCarInfo["km"])+" | BIDDING_END_DATE= "+str(biddingEndDate))
        else:
            setLogPrint("INFO: 오차범위 적은 CAR_ID="+str(carId)+"  | NOT FOUND -- Low Error Car List  | syncText: 시세 없음") 
    except Exception as e:
        print("전체 Traceback:")
        traceback.print_exc()         
    return lowErrorCarInfo
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
        setLogPrint("INFO: LOGIN_URL:"+LOGIN_URL)

        #2.2.로그인 입력
        wait.until(EC.presence_of_element_located((By.NAME, "username"))).send_keys(LOGIN_ID)
        driver.find_element(By.NAME, "password").send_keys(LOGIN_PW)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        #2.3.로그인 성공 확인
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '로그아웃')]")))
        setLogPrint("INFO: [✓]최초 로그인 처리 성공 ++++")
        secNumber = random.randint(5, 13)
        setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    else:
        driver.quit()
        setLogPrint("INFO: WebDriver ["+str(VAR_WD_DEFAULT_COUNT)+"]번쩨 초기화 START")
        driver = getProxyWebdriverInfo()
        wait = WebDriverWait(driver, 15)

        driver.get(LOGIN_URL)
        setLogPrint("INFO: LOGIN_URL:"+LOGIN_URL)

        #2.2.로그인 입력
        wait.until(EC.presence_of_element_located((By.NAME, "username"))).send_keys(LOGIN_ID)
        driver.find_element(By.NAME, "password").send_keys(LOGIN_PW)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()

        #2.3.로그인 성공 확인
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), '로그아웃')]")))
        setLogPrint("INFO: [✓]["+str(VAR_WD_DEFAULT_COUNT)+"]번째 로그인 처리 성공 ++++")
        secNumber = random.randint(3, 9)
        setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
        time.sleep(secNumber)
    return driver

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
    setLogPrint("INFO: getProxyWebdriverInfo START")
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
    setLogPrint("INFO: getProxyWebdriverInfo END")
    secNumber = random.randint(3, 6)
    setLogPrint(f"TIME: time.Sleep:: {secNumber} Sec")
    time.sleep(secNumber)
    return driver

#2.타겟 사이트 접속
try:
    driver = getWebDriverLoginProc("",1)
    setLogPrint("INFO: Login Complete > Crawlling Start")

    #DB에서 상세화면 크롤링 목록 가져오기
    dbConn = getDbConnectInfo()
    dbCursor = dbConn.cursor()
    try:
        dbCursor = dbConn.cursor()   # 실패 시 예외 발생
        setLogPrint("INFO: DB Connect Complete")
    except Exception as e:    
        print(f"ERROR: DB Cursor Connect Fail {type(e).__name__}: {e}")

    #사용 사용 빈도가 낮은 프록시 서버 아이피, 포트 가져오기
    #BIDDING_END_DATE 가 지난 건은 조회 불가능
    
    # query = '''
    #     UPDATE TBL_CAR_AUCTION_LIST
    #     SET BIDDING_STATUS ='시세수집대기'        
    #     WHERE BIDDING_END_DATE <=  DATE_FORMAT(NOW(),'%Y-%m-%d' )
    #     AND SITE_CODE='3'
    # '''
    # dbCursor.execute(query)
    # dbConn.commit()  ##물리적으로 데이터 저장을 확정시킴

    # 현재기준 3일 이전에 경매 종료기준, 경매상태: 시세수집대기, 배치상태: 2번(시세수집대기)
    query = '''
        SELECT 
            PRD_SEQ
            ,CAR_ID
            ,DETAIL_URL 
            ,BIDDING_END_DATE
            ,PRICE_LIST_URL
            ,YEARS
            ,KM
            ,SITE_CODE
            ,MODEL 
            ,MODEL_DETAIL
        FROM TBL_CAR_AUCTION_LIST
        WHERE SITE_CODE = '3'
        AND SYNC_STATUS ='2'
        AND SYNC_TEXT LIKE '%시세수집대기%'
        AND BIDDING_END_DATE  < DATE_FORMAT( DATE_ADD(NOW(), INTERVAL 0 DAY) ,'%Y-%m-%d' )
        AND INSTR(PRICE_LIST_URL,'dealers/web/price/cars') > 0
        AND LENGTH(YEARS) > 0 AND YEARS <> 'None'
        ORDER BY REG_DATE ASC
    '''

    connInfo = dbConn.cursor(pymysql.cursors.DictCursor)
    connInfo.execute(query)  #쿼리 실행
    result=connInfo.fetchall()
    
    df = pd.DataFrame(result)
    totalCount = len(df)
    setLogPrint(f"INFO: Target CarList Select complete >> TotalCount:{totalCount}")
    subCarListCount = 0    
    if len(df) > 0:

        procCount = 0
        defaultCount = 0
        dbReConnectCount = 0
        loginCount = 0
      
        for idx, row in df.iterrows():
            
            UPDATE_COUNT = 0
            INSERT_COUNT = 0
            subCarListCount = 0
            subPageNum = 1
            carInfo = {}
            # dict 형태라 키로 접근 가능
            #print(f"[{idx}] {row['PRD_SEQ']} | {row['CAR_ID']} | {row['DETAIL_URL']}")
            carInfo["prdSeq"]          = row['PRD_SEQ']
            carInfo["detailUrl"]       = row['DETAIL_URL']
            carInfo["carId"]           = row['CAR_ID']
            carInfo["biddingEndDate"]  = row['BIDDING_END_DATE']
            carInfo["priceListUrl"]    = row['PRICE_LIST_URL']
            carInfo["years"]           = row['YEARS']
            carInfo["km"]              = row['KM']
            carInfo["siteCode"]        = row['SITE_CODE']
            carInfo["model"]           = row['MODEL']
            carInfo["modelDetail"]     = row['MODEL_DETAIL']
            
            hdModelIdTemp = re.search(r'(?:^|[?&])model=([^&]*)', carInfo["priceListUrl"])
            hdGradeIdTemp = re.search(r'(?:^|[?&])grade=([^&]*)', carInfo["priceListUrl"])
            hdModelId = hdModelIdTemp.group(1) if hdModelIdTemp else None
            hdGradeId = hdGradeIdTemp.group(1) if hdGradeIdTemp else None

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

            targetUrl = carInfo["priceListUrl"].replace("VAR_PAGE_NUM",str(subPageNum))            
            getListData = getRemoveHtmlTags(getWebSpiderData(driver,targetUrl))
            
            secNumber = random.randint(3, 6)            
            time.sleep(secNumber)   
            priceList = json.loads(getListData)
         
            targetSubUrl = ""
            if "toast_message" in priceList:
                if not priceList.get("toast_message").find("로그인 후 사용"):
                    setLogPrint("ERROR: Duplicate login detected and stopped") 
                    driver.quit()                
                    exit(0)         

            while True:                
                targetSubUrl = carInfo["priceListUrl"].replace("VAR_PAGE_NUM",str(subPageNum))   
                setLogPrint("INFO: targetSubUrl:"+targetSubUrl)                
                getPriceListData = getRemoveHtmlTags(getWebSpiderData(driver,targetSubUrl))                    
                subCarList = json.loads(getPriceListData)
                
                if len(subCarList) > 0 :                    
                    subCarListCount+=1
                    for subCar in subCarList:  
                        try:                                
                            detailSub = subCar.get("detail", {})
                            auctionSub = subCar.get("auction", {})                                                 
                            
                            subCarInfo = {}
                            subCarInfo["carId"]=carInfo["carId"]
                            subCarInfo["modelId"]=hdModelId
                            subCarInfo["gradeId"]=hdGradeId
                            subCarInfo["biddingPrice"]=auctionSub.get("highest_bid", {}).get("price")
                            subCarInfo["biddingCount"]=auctionSub.get("bids_count")
                            subCarInfo["fuel"]=detailSub.get("fuel_display")
                            subCarInfo["gradeDetail"]=detailSub.get("grade_part_name")
                            subCarInfo["mission"]=detailSub.get("transmission_display")
                            subCarInfo["years"]=detailSub.get("year")
                            subCarInfo["km"]=detailSub.get("mileage")
                            subCarInfo["fromEndDate"]=auctionSub.get("ended_at_display")                                
                            accidentArray = detailSub.get("carhistory", {}).get("my_car_accident_summary").split('·')           
                            if isinstance(accidentArray, list) and len(accidentArray) > 1:
                                print("accidentArray:"+str(accidentArray))
                                subCarInfo["myAccidentPrice"]=accidentArray[1]
                                subCarInfo["myAccidentCount"]=accidentArray[0]
                            else:
                                subCarInfo["myAccidentCount"]=accidentArray  
                                subCarInfo["myAccidentPrice"]=""               
                            
                            subCarInfo["ownerChangedCount"]=detailSub.get("carhistory", {}).get("owner_changed_count")
                            subCarInfo["accidentList"]=str(detailSub.get("accident_repairs", {})).replace("'","''")
                            subCarInfo["siteCode"]=VAR_SITE_CODE                             
                            retData = setAuctionSubInsert(subCarInfo,dbCursor )
                        
                            if retData == "D":
                                UPDATE_COUNT+=1
                                #setLogPrint("INFO: == Not INSERT PRICE_LIST(SUB) CAR_ID:"+carInfo["carId"]+" | MODEL_ID:"+hdModelId+" | GRADE_ID:"+hdGradeId+" KM:"+carInfo["km"]  +" | YEARS:"+carInfo["years"]+" | BIDDING_PRICE:"+str(subCarInfo["biddingPrice"]))
                            else:
                                INSERT_COUNT+=1
                                #setLogPrint("INFO: ++ INSERT PRICE_LIST(SUB) CAR_ID:"+carInfo["carId"]+" | MODEL_ID:"+hdModelId+" | GRADE_ID:"+hdGradeId+" KM:"+carInfo["km"]  +" | YEARS:"+carInfo["years"]+" | BIDDING_PRICE:"+str(subCarInfo["biddingPrice"]))
                        except Exception as e:
                            print("=" * 50)
                            print("전체 Traceback:")
                            traceback.print_exc()
                    subPageNum+=1
                    dbConn.commit() 
                else:
                    setLogPrint("INFO: targetSubUrl End of Page:"+targetSubUrl)
                    break;            
                
            #오차범위 적은 차량 추출
            if subCarListCount > 0 :                
                lowErrorCarRetData = {}
                lowErrorCarRetData = getHighMatchingCarInfo(connInfo, carInfo["carId"], carInfo["years"], carInfo["km"],carInfo["biddingEndDate"] )                
                if lowErrorCarRetData :                    
                    carInfo["prdSubSeq"]=lowErrorCarRetData["prdSubSeq"]
                    carInfo["biddingPrice"]=lowErrorCarRetData["biddingPrice"]
                    carInfo["biddingStatus"] = "경매종료"  
                    carInfo["syncText"] = "시세수집완료(HD-PLP)"
                    carInfo["syncStatus"] = "3"                        
                else:                    
                    setLogPrint(f"INFO: HightMatchingCar Not Found Data >> carid:{carInfo['carId']} | years:{carInfo['years']} | km:{carInfo['km']} | biddingEndDate:{carInfo['biddingEndDate']}")
                    carInfo["prdSubSeq"]="0"
                    carInfo["biddingPrice"]="0"
                    carInfo["biddingStatus"] = "경매종료"
                    carInfo["syncText"] = "시세매칭실패(HD-PLP)"
                    carInfo["syncStatus"] = "4" 
                    
                #setLogPrint("###CarInfo:"+str(carInfo))                            
                setCarInfoDbUpdate(carInfo,dbCursor)
                #print("insert: carId:",carInfo["carId"])
                dbConn.commit()               
            else:
                setLogPrint("WARN: -- subCarList Not Found List[carId:"+carInfo["carId"]+"] > targetSubUrl:"+targetSubUrl)
                carInfo["prdSubSeq"]="0"
                carInfo["biddingPrice"]="0"
                carInfo["biddingStatus"] = "경매종료"
                carInfo["syncText"] = "시세없음(HD-PLP)"
                carInfo["syncStatus"] = "5" 
                
                #setLogPrint("###CarInfo:"+str(carInfo))                            
                setCarInfoDbUpdate(carInfo,dbCursor)
                #print("insert: carId:",carInfo["carId"])
                dbConn.commit()
             
            procPercent = round(procCount/totalCount*100,1)
            secNumber = random.randint(2, 5) 
            setLogPrint(f"TIME: >>> time.Sleep:: {secNumber} Sec")            
            time.sleep(secNumber)          
            setLogPrint("INFO: >>> 처리현황 "+str(procCount)+"건 완료("+str(procPercent)+" %) / 전체 "+str(totalCount)+"건 - carId: "+carInfo["carId"]+" | insertCount:"+str(INSERT_COUNT)+" | updateCount:"+str(UPDATE_COUNT) )
            setLogPrint("--------------------------------------------------- NEXT CAR ---------------------------------------------------")
        setLogPrint("WARN: Not Found Select DB Data")
        driver.quit()
finally:
    connInfo.close() ## 연결 정도 자원 반환
    dbConn.close()
    driver.quit()
    df.head()
    if not df.empty:                
        df.describe()

    setLogPrint("END")
    
END_TIME = datetime.now()
TIME_GAP = END_TIME - START_TIME
elapsed_minutes = TIME_GAP.total_seconds()/60
gapTime = f"{elapsed_minutes:.2f}"

setLogPrint("######## END 소요시간:"+str(gapTime)+"분 ############")
