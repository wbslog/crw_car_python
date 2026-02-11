"""
KB차차차 무한스크롤 API 크롤러
================================
- MariaDB에서 프록시 IP/PORT 조회 → SOCKS5 우회 접속
- infinitySearch.json API 호출 → result > hits 에서 차량 정보 추출
- 저장 프로세스:
    1) 크롤링 (infinitySearch.json)
    2) TBL_CAR_PRODUCT_LIST에서 fullName + yymm 으로 스펙 조회
    3) car_seq 로 기등록 여부 검사
    4) 미등록 → INSERT / 기등록 → UPDATE(가격, SYNC_STATUS='3')
- hits가 빈 배열이 될 때까지 반복
"""

import requests
import json
import time
import random
import pymysql
import varList
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════════════

# ── MariaDB 접속 정보 ──
DB_CONFIG = {
    "host": varList.dbServerHost,
    "port": varList.dbServerPort,
    "user": varList.dbUserId,
    "password": varList.dbUserPass,
    "database": varList.dbServerName,
    "charset": "utf8mb4",
}

# ── SOCKS5 고정 인증 정보 ──
SOCKS5_USER = varList.proxyUserId
SOCKS5_PASS = varList.proxyUserPass

# ── 고정값 ──
SITE_CODE = "2000"

# ── 프록시 조회 쿼리 ──
PROXY_SELECT_SQL = "SELECT PROXY_IP, PROXY_PORT FROM NIF_PROXY_LIST WHERE STATUS = '1' ORDER BY  EXEC_COUNT ASC,  RAND() LIMIT 1"

# ──────────────────────────────────────────────
# 저장 프로세스 SQL
# ──────────────────────────────────────────────

# 2) 스펙 조회: fullName + yymm 으로 기존 등록 차량에서 스펙 가져오기
SPEC_SELECT_SQL = """
    SELECT
        DOMESTIC,
        KIND,
        MODEL,
        MODEL_DETAIL,
        GRADE,
        GRADE_DETAIL,
        MISSION,
        FUEL,
        AP_MODEL_ID,
        NEW_PRICE,
        MAKE_PRICE
    FROM TBL_CAR_PRODUCT_LIST
    WHERE STATUS = '1'
      AND SITE_CODE = '2000'
      AND FULL_NAME = %s
      AND YEARS = %s
    LIMIT 1
"""

# 3) 기등록 검사: car_seq 로 이미 있는지 확인
EXIST_CHECK_SQL = """
    SELECT COUNT(*) AS CNT
    FROM TBL_CAR_PRODUCT_LIST
    WHERE STATUS = '1'
      AND CAR_ID = %s
"""

# 4-A) INSERT: 신규 차량
INSERT_SQL = """
    INSERT INTO TBL_CAR_PRODUCT_LIST (
        SITE_CODE, CAR_ID, MODEL_DETAIL_ORI, YEARS, FIRST_DATE,
        KM, PRICE, STATUS, ADD_DATE, ADD_YMD, ADD_HOUR,
        FULL_NAME, DETAIL_URL, SYNC_STATUS, SYNC_TEXT,
        DOMESTIC, KIND, MAKER, MODEL, MODEL_DETAIL,
        GRADE, GRADE_DETAIL, COLOR, MISSION, AP_MODEL_ID,
        NEW_PRICE, MAKE_PRICE,
        MAKER_ORI, MODEL_ORI, GRADE_ORI, COLOR_ORI, FUEL_ORI
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, '1', NOW(),
        DATE_FORMAT(NOW(), '%%Y%%m%%d'),
        DATE_FORMAT(NOW(), '%%H'),
        %s, %s, '1', 'LP수집완료',
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s, %s
    )
"""

# 4-B) UPDATE: 기등록 차량 → 가격 갱신 + SYNC_STATUS='3'
UPDATE_SQL = """
    UPDATE TBL_CAR_PRODUCT_LIST
    SET PRICE = %s,
        MOD_YMD = DATE_FORMAT(NOW(), '%%Y%%m%%d'),
        MOD_HOUR = DATE_FORMAT(NOW(), '%%H'),
        SYNC_STATUS = '3'
    WHERE STATUS = '1'
      AND SITE_CODE = %s
      AND CAR_ID = %s
"""

# ── API 설정 ──
BASE_URL = "https://m.kbchachacha.com/public/web/search/infinitySearch.json"
DETAIL_URL = "https://www.kbchachacha.com/public/car/detail.kbc?carSeq="

INCLUDE_FIELDS = (
    "carSeq%2CfileNameArray%2CownerYn%2CmakerName%2CclassName%2CcarName%2CmodelName"
    "%2CgradeName%2CregiDay%2Cyymm%2Ckm%2CcityCodeName2%2CsellAmtGbn%2CsellAmt"
    "%2CsellAmtPrev%2CcarMasterSpecialYn%2CmonthLeaseAmt%2CinterestFreeYn%2CownerYn"
    "%2CdirectYn%2CcarAccidentNo%2CwarrantyYn%2CfalsityYn%2CkbLeaseYn%2CfriendDealerYn"
    "%2CorderDate%2CcertifiedShopYn%2CkbCertifiedYn%2ChasOverThreeFileNames%2CdiagYn"
    "%2CdiagGbn%2ClineAdYn%2CtbMemberMemberName%2CcarAccidentNo%2CcolorCodeName"
    "%2CgasName%2CsafeTel%2CcarHistorySeq%2ChomeserviceYn2%2ClabsDanjiNo2%2CpremiumYn"
    "%2CpremiumVideo%2CpremiumVideoType%2CpremiumVideoImage%2Ct34SellGbn%2Ct34MonthAmt"
    "%2Ct34DiscountAmt%2CadState%2CshopPenaltyYn%2CpaymentPremiumYn%2CpaymentPremiumText"
    "%2CpaymentPremiumMarkCdArray%2CpaymentPremiumMarkNmArray%2CcontractingYn"
    "%2CpartnerCertifiedYn%2CseatColorCode%2CseatColorNm%2CpaymentPlayYn"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://m.kbchachacha.com/public/web/search/list.kbc",
}


# ══════════════════════════════════════════════════════════════
# DB 함수
# ══════════════════════════════════════════════════════════════

def get_db_connection():
    """MariaDB 연결 반환"""
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        charset=DB_CONFIG["charset"],
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_proxy_from_db():
    """MariaDB에서 프록시 IP, PORT 1건 랜덤 조회"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(PROXY_SELECT_SQL)
            row = cur.fetchone()
        if row:
            ip = row.get("PROXY_IP", "")
            port = row.get("PROXY_PORT", "")
            print("  ✅ 프록시 조회: " + str(ip) + ":" + str(port))
            return ip, int(port)
        else:
            print("  ⚠ 프록시 조회 결과 없음")
            return None, None
    finally:
        conn.close()


# ──────────────────────────────────────────────
# 저장 프로세스 DB 함수
# ──────────────────────────────────────────────

def lookup_spec(full_name, yymm):
    """
    [프로세스 2] 스펙 조회
    TBL_CAR_PRODUCT_LIST에서 FULL_NAME + YEARS 로 기존 스펙 조회.
    매칭 있으면 dict 반환, 없으면 None.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SPEC_SELECT_SQL, (full_name, yymm))
            return cur.fetchone()
    finally:
        conn.close()


def check_car_exists(car_seq):
    """
    [프로세스 3] 기등록 검사
    CAR_ID 로 이미 등록된 차량인지 확인.
    Returns: True(기등록) / False(미등록)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(EXIST_CHECK_SQL, (car_seq,))
            row = cur.fetchone()
        cnt = row.get("CNT", 0) if row else 0
        return cnt > 0
    finally:
        conn.close()


def insert_car(car_info):
    """
    [프로세스 4-A] 신규 차량 INSERT
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, (
                car_info["siteCode"],       # SITE_CODE
                car_info["carId"],          # CAR_ID
                car_info["modelDetailOri"], # MODEL_DETAIL_ORI
                car_info["years"],          # YEARS
                car_info["firstDate"],      # FIRST_DATE
                car_info["km"],             # KM
                car_info["price"],          # PRICE
                car_info["fullName"],       # FULL_NAME
                car_info["detailUrl"],      # DETAIL_URL
                car_info["domestic"],       # DOMESTIC
                car_info["kind"],           # KIND
                car_info["maker"],          # MAKER (= maker_name from spec)
                car_info["model"],          # MODEL
                car_info["modelDetail"],    # MODEL_DETAIL
                car_info["grade"],          # GRADE
                car_info["gradeDetail"],    # GRADE_DETAIL
                car_info["color"],          # COLOR
                car_info["mission"],        # MISSION
                car_info["apModelId"],      # AP_MODEL_ID
                car_info["newPrice"],       # NEW_PRICE
                car_info["makePrice"],      # MAKE_PRICE
                car_info["makerOri"],       # MAKER_ORI
                car_info["modelOri"],       # MODEL_ORI
                car_info["gradeOri"],       # GRADE_ORI
                car_info["colorOri"],       # COLOR_ORI
                car_info["fuelOri"],        # FUEL_ORI
            ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print("    ✗ INSERT 실패: " + str(e))
        return False
    finally:
        conn.close()


def update_car(car_seq, price):
    """
    [프로세스 4-B] 기등록 차량 UPDATE
    가격 갱신 + SYNC_STATUS='3'
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(UPDATE_SQL, (price, SITE_CODE, car_seq))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print("    ✗ UPDATE 실패: " + str(e))
        return False
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════
# 크롤링 함수 (기존 로직 유지)
# ══════════════════════════════════════════════════════════════

def create_session_with_proxy(proxy_ip, proxy_port):
    """SOCKS5 프록시 세션 생성"""
    session = requests.Session()

    if proxy_ip and proxy_port:
        proxy_url = "socks5h://" + SOCKS5_USER + ":" + SOCKS5_PASS \
                    + "@" + str(proxy_ip) + ":" + str(proxy_port)
        session.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        print("  ✅ SOCKS5 프록시 설정: " + str(proxy_ip) + ":" + str(proxy_port))
    else:
        print("  ⚠ 프록시 없이 직접 연결")

    # 세션 쿠키 확보
    print("  ▶ 세션 초기화...")
    session.get(
        "https://m.kbchachacha.com/public/web/search/list.kbc",
        headers={"User-Agent": HEADERS["User-Agent"]},
        timeout=15,
    )

    return session


def build_url(page, page_size, search_after_arr):
    """GET 쿼리스트링 조립"""
    v = str(int(time.time() * 1000))

    url = BASE_URL \
        + "?sort=-paymentPlayYn%2C-orderDate" \
        + "&page=" + str(page) \
        + "&pageSize=" + str(page_size) \
        + "&includeFields=" + INCLUDE_FIELDS \
        + "&displaySoldoutYn=Y" \
        + "&v=" + v \
        + "&paymentPremiumYn=Y"

    if search_after_arr and isinstance(search_after_arr, list):
        for val in search_after_arr:
            url = url + "&searchAfter=" + str(val)
    else:
        url = url + "&searchAfter="

    return url


def fetch_page(session, page=1, page_size=30, search_after_arr=None):
    """단일 페이지 GET 호출"""
    url = build_url(page, page_size, search_after_arr)

    resp = session.get(url, headers=HEADERS, timeout=15)

    if resp.status_code != 200:
        print("  ✗ 상태코드: " + str(resp.status_code))
        return [], None

    try:
        data = resp.json()
    except json.JSONDecodeError:
        print("  ✗ JSON 파싱 실패")
        return [], None

    result = data.get("result", {})
    hits = result.get("hits", [])

    next_search_after = result.get("searchAfter", [])
    if not next_search_after or not isinstance(next_search_after, list):
        next_search_after = None

    return hits, next_search_after


# ══════════════════════════════════════════════════════════════
# 저장 프로세스 핵심 함수
# ══════════════════════════════════════════════════════════════

def format_first_date(regi_day):
    """
    regiDay → 'YYYY-MM-01' 형식 변환
    예: '202301' → '2023-01-01'
        '2301'   → '2023-01-01'
        '2023-01' → '2023-01-01'
    """
    if not regi_day:
        return ""

    rd = str(regi_day).strip().replace("-", "").replace("/", "").replace(".", "")

    # YYYYMM (6자리)
    if len(rd) == 6 and rd.isdigit():
        return rd[:4] + "-" + rd[4:6] + "-01"

    # YYMM (4자리)
    if len(rd) == 4 and rd.isdigit():
        yy = int(rd[:2])
        prefix = "20" if yy < 80 else "19"
        return prefix + rd[:2] + "-" + rd[2:4] + "-01"

    # YYYYMMDD (8자리)
    if len(rd) == 8 and rd.isdigit():
        return rd[:4] + "-" + rd[4:6] + "-01"

    return ""


def build_car_info(car, spec):
    """
    API 응답(car) + 스펙 조회(spec) → INSERT용 car_info dict 조립

    컬럼 매핑:
      SITE_CODE        = "2000" (고정)
      CAR_ID           = car_seq
      MODEL_DETAIL_ORI = maker_name + class_name + model_name + grade_name
      YEARS            = yymm
      FIRST_DATE       = regi_day → YYYY-MM-01
      KM               = km
      PRICE            = sell_amt
      FULL_NAME        = maker_name + class_name + model_name + grade_name
      MAKER_ORI        = maker_name
      MODEL_ORI        = model_name
      GRADE_ORI        = grade_name
      COLOR_ORI        = colorCodeName
      FUEL_ORI         = gas_name
      DOMESTIC ~ MAKE_PRICE = 스펙 조회 결과 (없으면 빈값)
    """
    car_seq    = str(car.get("carSeq", ""))
    maker_name = car.get("makerName", "") or ""
    class_name = car.get("className", "") or ""
    model_name = car.get("modelName", "") or ""
    grade_name = car.get("gradeName", "") or ""
    yymm       = car.get("yymm", "") or ""
    regi_day   = car.get("regiDay", "") or ""
    km         = car.get("km", 0) or 0
    gas_name   = car.get("gasName", "") or ""
    color_name = car.get("colorCodeName", "") or ""
    sell_amt   = car.get("sellAmt", 0) or 0

    full_name = (maker_name + " " + class_name + " " + model_name + " " + grade_name).strip()
    detail_url = DETAIL_URL + car_seq
    first_date = format_first_date(regi_day)

    # 스펙 조회 결과 반영 (없으면 빈값)
    if spec:
        domestic     = spec.get("DOMESTIC", "") or ""
        kind         = spec.get("KIND", "") or ""
        model        = spec.get("MODEL", "") or ""
        model_detail = spec.get("MODEL_DETAIL", "") or ""
        grade        = spec.get("GRADE", "") or ""
        grade_detail = spec.get("GRADE_DETAIL", "") or ""
        mission      = spec.get("MISSION", "") or ""
        color        = spec.get("FUEL", "") or ""
        ap_model_id  = spec.get("AP_MODEL_ID", "") or ""
        new_price    = spec.get("NEW_PRICE", 0) or 0
        make_price   = spec.get("MAKE_PRICE", 0) or 0
    else:
        domestic     = ""
        kind         = ""
        model        = ""
        model_detail = ""
        grade        = ""
        grade_detail = ""
        mission      = ""
        color        = ""
        ap_model_id  = ""
        new_price    = 0
        make_price   = 0

    return {
        "siteCode":       SITE_CODE,
        "carId":          car_seq,
        "modelDetailOri": full_name,
        "years":          yymm,
        "firstDate":      first_date,
        "km":             km,
        "price":          sell_amt,
        "fullName":       full_name,
        "detailUrl":      detail_url,
        "domestic":       domestic,
        "kind":           kind,
        "maker":          maker_name,
        "model":          model,
        "modelDetail":    model_detail,
        "grade":          grade,
        "gradeDetail":    grade_detail,
        "color":          color,
        "mission":        mission,
        "apModelId":      ap_model_id,
        "newPrice":       new_price,
        "makePrice":      make_price,
        "makerOri":       maker_name,
        "modelOri":       model_name,
        "gradeOri":       grade_name,
        "colorOri":       color_name,
        "fuelOri":        gas_name,
    }


def process_car(car):
    """
    차량 1대 저장 프로세스:
      [프로세스 2] fullName + yymm 으로 스펙 조회
      [프로세스 3] car_seq 로 기등록 검사
      [프로세스 4] CNT=0 → INSERT / CNT>0 → UPDATE
    Returns: "insert" / "update" / "error"
    """
    car_seq    = str(car.get("carSeq", ""))
    maker_name = car.get("makerName", "") or ""
    class_name = car.get("className", "") or ""
    model_name = car.get("modelName", "") or ""
    grade_name = car.get("gradeName", "") or ""
    yymm       = car.get("yymm", "") or ""
    sell_amt   = car.get("sellAmt", 0) or 0

    full_name = (maker_name + " " + class_name + " " + model_name + " " + grade_name).strip()

    # ── [프로세스 2] 스펙 조회 ──
    spec = lookup_spec(full_name, yymm)

    # ── [프로세스 3] 기등록 검사 ──
    exists = check_car_exists(car_seq)

    if exists:
        # ── [프로세스 4-B] UPDATE: 가격 갱신 + SYNC_STATUS='3' ──
        ok = update_car(car_seq, sell_amt)
        return "update" if ok else "error"
    else:
        # ── [프로세스 4-A] INSERT: 신규 등록 ──
        car_info = build_car_info(car, spec)
        ok = insert_car(car_info)
        return "insert" if ok else "error"


# ══════════════════════════════════════════════════════════════
# 출력 함수
# ══════════════════════════════════════════════════════════════

def print_car(idx, car, action):
    """차량 정보 + 처리결과 콘솔 출력"""
    car_seq    = car.get("carSeq", "")
    car_name   = car.get("carName", "")
    maker_name = car.get("makerName", "")
    model_name = car.get("modelName", "")
    grade_name = car.get("gradeName", "")
    yymm       = car.get("yymm", "")
    regi_day   = car.get("regiDay", "")
    km         = car.get("km", "")
    sell_amt   = car.get("sellAmt", "")
    gas_name   = car.get("gasName", "")
    detail_url = DETAIL_URL + str(car_seq)

    km_str  = "{:,}km".format(km) if isinstance(km, (int, float)) else str(km) + "km"
    amt_str = "{:,}만원".format(sell_amt) if isinstance(sell_amt, (int, float)) else str(sell_amt) + "만원"

    if action == "insert":
        tag = "🆕INSERT"
    elif action == "update":
        tag = "🔄UPDATE"
    else:
        tag = "❌ERROR"

    print("[" + str(idx) + "] " + tag
          + " | 상품코드: " + str(car_seq)
          + " | 제조사: " + str(maker_name)
          + " | 차량명: " + str(car_name)
          + " | 모델: " + str(model_name)
          + " | 등급: " + str(grade_name)
          + " | 연식: " + str(yymm)
          + " | 등록년월: " + str(regi_day)
          + " | 주행거리: " + km_str
          + " | 판매가: " + amt_str
          + " | 연료: " + str(gas_name))
    print("         상세링크: " + detail_url)
    print("─" * 60)


# ══════════════════════════════════════════════════════════════
# 메인 실행
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    
    START_TIME = datetime.now()
    
    print("=" * 70)
    print("  KB차차차 infinitySearch.json 크롤러")
    print("  (SOCKS5 프록시 / TBL_CAR_PRODUCT_LIST 저장)")
    print("=" * 70)

    # 1) MariaDB에서 프록시 조회
    print("\n▶ 프록시 조회...")
    proxy_ip, proxy_port = get_proxy_from_db()

    # 2) SOCKS5 프록시 세션 생성
    print("\n▶ 세션 생성...")
    session = create_session_with_proxy(proxy_ip, proxy_port)

    # 3) 무한스크롤 수집 시작
    page = 1
    total_count = 0
    insert_count = 0
    update_count = 0
    error_count = 0
    search_after_arr = None

    while True:
        WHILE_TIME = datetime.now()
        print("\n" + "━" * 70)
        print("▶ 페이지 " + str(page)
              + " | searchAfter = "
              + (str(search_after_arr) if search_after_arr else "(첫 페이지)"))
        print("━" * 70)

        hits, next_arr = fetch_page(
            session, page=page, page_size=30, search_after_arr=search_after_arr
        )

        # hits가 비어있으면 종료
        if not hits:
            print("\n📌 hits 비어있음 → 수집 종료")
            break

        # ── 차량별 저장 프로세스 실행 ──
        for car in hits:
            total_count += 1

            try:
                action = process_car(car)
            except Exception as e:
                print("    ✗ 처리 오류 (carSeq=" + str(car.get("carSeq", "")) + "): " + str(e))
                action = "error"

            if action == "insert":
                insert_count += 1
            elif action == "update":
                update_count += 1
            else:
                error_count += 1

            print_car(total_count, car, action)

        print("\n  ── 이번 페이지: " + str(len(hits)) + "대"
              + " | 누적: " + str(total_count) + "대"
              + " (INSERT: " + str(insert_count)
              + " / UPDATE: " + str(update_count)
              + " / ERROR: " + str(error_count) + ") ──")

        # 다음 searchAfter 없으면 종료
        if next_arr is None:
            print("\n📌 searchAfter 없음 → 마지막 페이지")
            break

        print("  → 다음 searchAfter: " + str(next_arr))
        search_after_arr = next_arr

        # 5~10초 랜덤 대기
        PAUSE_TIME = datetime.now()
        PAUSE_TIME_GAP = PAUSE_TIME - START_TIME
        elapsed_minutes = PAUSE_TIME_GAP.total_seconds()/60
        gapTime = f"{elapsed_minutes:.2f}"
        
        WHILE_TIME_GAP = PAUSE_TIME - WHILE_TIME
        elapsed_minutes = WHILE_TIME_GAP.total_seconds()/60
        whileGapTime = f"{elapsed_minutes:.2f}"
       
        
        wait = random.uniform(5, 10)
        print("  ⏳ " + "{:.1f}".format(wait) + "초 대기 중...")
        print("  ⏳ 처리소요시간: "+str(whileGapTime) +" Sec") 
        print("  ⏳ 총 경과시간: "+str(gapTime) +" Sec") 
        time.sleep(wait)

    END_TIME = datetime.now()
    TIME_GAP = END_TIME - START_TIME
    elapsed_minutes = TIME_GAP.total_seconds()/60
    gapTime = f"{elapsed_minutes:.2f}"
    
    
    # 최종 결과
    print("\n" + "=" * 70)
    print("📊 수집 완료!")
    print("   총 수집: " + str(total_count) + "대")
    print("   INSERT:  " + str(insert_count) + "건 (신규)")
    print("   UPDATE:  " + str(update_count) + "건 (가격 갱신)")
    print("   ERROR:   " + str(error_count) + "건")
    print("   Total tile: "+str(gapTime) +" Sec") 
    print("=" * 70)
    
    