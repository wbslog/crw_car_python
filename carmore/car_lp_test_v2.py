from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

class WebScraper:
    def __init__(self, chromedriver_path=None):
        """
        웹 스크래퍼 초기화
        :param chromedriver_path: ChromeDriver 경로 (None이면 자동 탐색)
        """
        self.driver = None
        self.cookies = None
        self.session = requests.Session()
        self.chromedriver_path = chromedriver_path
        
    def setup_driver(self, headless=False):
        """
        Chrome WebDriver 설정
        :param headless: 헤드리스 모드 사용 여부
        """
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        if self.chromedriver_path:
            service = Service(self.chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            self.driver = webdriver.Chrome(options=chrome_options)
        
        self.driver.implicitly_wait(10)
        
    def access_main_page(self, url, wait_time=3):
        """
        메인 페이지 접속
        :param url: 접속할 URL
        :param wait_time: 페이지 로드 대기 시간 (초)
        """
        print(f"메인 페이지 접속 중: {url}")
        self.driver.get(url)
        time.sleep(wait_time)
        print("페이지 로드 완료")
        
    def get_cookies(self):
        """
        현재 브라우저의 쿠키 정보 수집
        :return: 쿠키 딕셔너리
        """
        self.cookies = self.driver.get_cookies()
        print(f"수집된 쿠키 개수: {len(self.cookies)}")
        
        # 쿠키 정보 출력
        for cookie in self.cookies:
            print(f"- {cookie['name']}: {cookie['value'][:50]}...")
        
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
    
    def scrape_with_driver(self, url):
        """
        Selenium 드라이버를 사용하여 페이지 수집
        :param url: 수집할 URL
        :return: 페이지 소스
        """
        print(f"\n드라이버로 페이지 수집 중: {url}")
        self.driver.get(url)
        time.sleep(2)
        
        page_source = self.driver.page_source
        print(f"페이지 소스 길이: {len(page_source)} 문자")
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
            username_field = WebDriverWait(self.driver, 10).until(
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
            print(f"로그인 중 오류 발생: {e}")
    
    def close(self):
        """
        드라이버 종료
        """
        if self.driver:
            self.driver.quit()
            print("드라이버 종료됨")


def extract_model_data_from_html(html_source):
    """
    HTML 소스에서 data-model 속성을 가진 <a> 태그 추출 (BeautifulSoup 사용)
    
    :param html_source: HTML 소스 코드
    :return: 추출된 데이터 리스트
    """
    soup = BeautifulSoup(html_source, 'html.parser')
    
    # data-model 속성을 가진 모든 <a> 태그 찾기
    elements = soup.find_all('a', attrs={'data-model': True})
    
    # data-model 값만 추출
    model_values = []
    for element in elements:
        model_value = element.get('data-model')
        if model_value:
            model_values.append(model_value)     
            
    print("#######",str(model_values))  
    #extracted_data = []
        
    #print(f"\n총 {len(model_links)}개의 data-model 태그를 찾았습니다.\n")
        
    # for idx, link in enumerate(model_links, 1):
    #     data = {
    #         'index': idx,
    #         'model': link.get('data-model'),
    #         'model_id': link.get('data-model-id'),
    #         'text': link.get_text(strip=True),
    #         'href': link.get('href')
    #     }
        
    #     extracted_data.append(data)
        
    #     print(f"[{idx}] 모델명: {data['model']}, 모델ID: {data['model_id']}, 텍스트: {data['text']}")
    
    # return extracted_data

    

# 사용 예시
if __name__ == "__main__":
    # 스크래퍼 인스턴스 생성
    scraper = WebScraper()
    
    try:
        # 1. 드라이버 설정
        scraper.setup_driver(headless=True)
        
        # 2. 메인 페이지 접속
        main_url = "https://carmore.kr/home/?modal=mainPopup"  # 실제 URL로 변경
        scraper.access_main_page(main_url)
        
        # 3. 로그인이 필요한 경우 (선택사항)
        # scraper.login_if_needed(
        #     username="your_username",
        #     password="your_password",
        #     username_selector="#username",
        #     password_selector="#password",
        #     submit_selector="button[type='submit']"
        # )
        
        # 4. 쿠키 및 세션 정보 수집
        cookies = scraper.get_cookies()
        session_storage = scraper.get_session_storage()
        local_storage = scraper.get_local_storage()
        
        # 5. 헤더 생성
        headers = scraper.create_headers_with_cookies()
        
        # 6. 데이터 수집 방법 1: Selenium 드라이버 사용
        now = datetime.now()
    
        one_day_later = now + timedelta(days=1)
        two_day_later = now + timedelta(days=2)        

        startDate = one_day_later.strftime('%Y-%m-%d')
        startTime = one_day_later.strftime('%H:%M:%S')
        
        endDate = two_day_later.strftime('%Y-%m-%d')
        endTime = two_day_later.strftime('%H:%M:%S')

        # 수집할 페이지 URL
        target_url = f"https://carmore.kr/home/carlist.html?areaCode=Q_1&rentStartDate={startDate}%20{startTime}&rentEndDate={endDate}%20{endTime}&isOverseas=false&nationalCode=KR&rt=1&locationName=%EC%A0%9C%EC%A3%BC%EA%B5%AD%EC%A0%9C%EA%B3%B5%ED%95%AD&sls=5"
    
        page_source = scraper.scrape_with_driver(target_url)
        
        # 7. 데이터 수집 방법 2: requests 라이브러리 사용 (더 빠름)
        #response = scraper.scrape_with_requests(target_url, headers)
        
        #print(page_source)
        
        data = extract_model_data_from_html(page_source)
       
        # 8. 수집된 데이터 저장
        saveFilePath ="D:\\data\\claude\\scraped_data.html"
        with open(saveFilePath, 'w', encoding='utf-8') as f:
            f.write(page_source)
        print("\n데이터가 scraped_data.html에 저장되었습니다.")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 9. 드라이버 종료
        scraper.close()