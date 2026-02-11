# CarMore 사이트 수집 테스트
# 1.LP 수집
#  > 차량명, 상세 링크 수집
# 2.VIP 수집
#  > 차량 세부 정보 수집( 업체별, 년식별, 가격정보 )

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup


USER_AGENT_NAME = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.7444.60 Safari/537.36"



def crawl_carmore_rentals(url):
    """
    Carmore 렌터카 목록을 크롤링하는 함수
    """
    # Chrome 옵션 설정
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 브라우저 창을 띄우지 않음 (원하면 주석 처리)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f'user-agent='+USER_AGENT_NAME)
    
    # 드라이버 초기화
    driver = webdriver.Chrome(options=options)
    
    try:
        print("페이지 로딩 중...")
        driver.get(url)
       
        # 더보기 버튼 계속 클릭
        click_count = 0
        while True:
            try:
                # 더보기 버튼 찾기 (여러 선택자 시도)
                more_button = None
                selectors = [
                    "button.more-btn",
                    "button[class*='more']",
                    "a.more-btn",
                    "//button[contains(text(), '더 보기')]",
                    "//a[contains(text(), '더 보기')]",
                    ".btn-more",
                    "#moreBtn"
                ]
                
                for selector in selectors:                    
                    try:
                        if selector.startswith("//"):
                            more_button = WebDriverWait(driver, 2).until(
                                EC.element_to_be_clickable((By.XPATH, selector))
                            )                         
                        else:
                            more_button = WebDriverWait(driver, 2).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                            )                          
                        break
                    except:
                        continue
                
                if more_button:
                    # 버튼이 보이도록 스크롤
                    driver.execute_script("arguments[0].scrollIntoView(true);", more_button)
                    time.sleep(0.5)
                    
                    # 클릭
                    more_button.click()
                    click_count += 1
                    print(f"더보기 버튼 클릭: {click_count}회")
                    
                    # 로딩 대기
                    time.sleep(2)
                else:
                    print("더보기 버튼을 찾을 수 없음. 종료합니다.")
                    break
                    
            except TimeoutException:
                print("더 이상 더보기 버튼이 없습니다. 크롤링 시작...")
                break
            except Exception as e:
                print(f"더보기 클릭 중 오류: {e}")
                break
        
        # 차량 목록 파싱
        cars_data = []
        
        # 차량 카드 찾기 (여러 선택자 시도)
        car_elements = []
        card_selectors = [           
            "a[data-model]"
        ]
        
        for selector in card_selectors:
            try:
                car_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if car_elements:
                    print(f"차량 요소 발견: {len(car_elements)}개")
                    break
            except:
                continue
        
        html_source = driver.page_source
        soup = BeautifulSoup(html_source, 'html.parser')
    
        # data-model 속성을 가진 모든 <a> 태그 찾기
        elements = soup.find_all('a', attrs={'data-model': True})
        
        # data-model 값만 추출
        model_names = []
        model_ids = []
        model_links = []
        for element in elements:
            model_name = element.get('data-model')
            model_id = element.get('data-model-id')
            model_link = element.get('href')
            if model_name:
                model_names.append(model_name)  
            if model_id:
                model_ids.append(model_id)  
            if model_name:
                model_links.append(model_link)
            
        print("####### model_name ######",str(model_names))  
        print("####### model_ids ######",str(model_ids))  
        print("####### model_links ######",str(model_links))  
        
      
        # # 각 차량 정보 추출
        # for idx, car in enumerate(car_elements, 1):
        #     try:
        #         car_info = {}
                
        #         # 차량명
        #         try:
        #             # model-data 속성을 가진 a 태그 찾기
        #             car_element = car.find_element(By.CSS_SELECTOR, "a[data-model]")
                    
        #             # model-data 속성 값 추출
        #             car_name = car_element.get_attribute("data-model").strip()
        #             car_info['차량명'] = car_name
        #             print("modelName:"+car_name)
        #         except:
        #             print("modelName:정보없음")
        #             car_info['차량명'] = "정보없음"
                
        #         # 렌터카 회사
        #         try:
        #             company = car.find_element(By.CSS_SELECTOR, ".company-name, .brand-name").text
        #             car_info['렌터카회사'] = company
        #         except:
        #             car_info['렌터카회사'] = "정보없음"
                
        #         # 가격
        #         try:
        #             price = car.find_element(By.CSS_SELECTOR, ".price, .amount, [class*='price']").text
        #             car_info['가격'] = price
        #         except:
        #             car_info['가격'] = "정보없음"
                
        #         # 차량 옵션/정보
        #         try:
        #             options = car.find_element(By.CSS_SELECTOR, ".car-option, .spec, .info").text
        #             car_info['옵션'] = options
        #         except:
        #             car_info['옵션'] = "정보없음"
                
        #         # HTML 전체 (디버깅용)
        #         car_info['HTML'] = car.get_attribute('outerHTML')[:5000]
                
        #         cars_data.append(car_info)
        #         print(f"{idx}. {car_info['차량명']} - {car_info['가격']}")
                
        #     except Exception as e:
        #         #print(f"차량 {idx} 파싱 오류: {e}")
        #         continue
        
        # print(f"\n총 {len(cars_data)}대의 차량 정보를 수집했습니다.")
        
        # DataFrame으로 변환
        df = pd.DataFrame(cars_data)
        
        # CSV 저장
        #output_file = 'carmore_rentals.csv'
        #df.to_csv(output_file, index=False, encoding='utf-8-sig')
        #print(f"\n결과가 '{output_file}' 파일로 저장되었습니다.")
        
        return df
        
    except Exception as e:
        print(f"오류 발생: {e}")
        return None
        
    finally:
        driver.quit()


if __name__ == "__main__":
    # 크롤링할 URL
    now = datetime.now()
    
    one_day_later = now + timedelta(days=1)
    two_day_later = now + timedelta(days=2)
    print(f"\n1일 뒤: {one_day_later}")
    print(f"1일 뒤 (포맷): {one_day_later.strftime('%Y년 %m월 %d일 %H시 %M분 %S초')}")


    startDate = one_day_later.strftime('%Y-%m-%d')
    startTime = one_day_later.strftime('%H:%M:%S')
    
    endDate = two_day_later.strftime('%Y-%m-%d')
    endTime = two_day_later.strftime('%H:%M:%S')
    
    url = f"https://carmore.kr/home/carlist.html?areaCode=Q_1&rentStartDate={startDate}%20{startTime}&rentEndDate={endDate}%20{endTime}&isOverseas=false&nationalCode=KR&rt=1&locationName=%EC%A0%9C%EC%A3%BC%EA%B5%AD%EC%A0%9C%EA%B3%B5%ED%95%AD&sls=5"
    
    # 크롤링 실행
    result = crawl_carmore_rentals(url)
    
    # 결과 출력
    # if result is not None and not result.empty:
    #     print("\n=== 수집된 데이터 미리보기 ===")
    #     print(result[['차량명', '렌터카회사', '가격']].head(10))
    
    
#LP:  https://carmore.kr/home/carlist.html?areaCode=Q_1&rentStartDate=2025-11-05%2010:00:00&rentEndDate=2025-11-06%2010:00:00&isOverseas=false&nationalCode=KR&rt=1&locationName=%EC%A0%9C%EC%A3%BC%EA%B5%AD%EC%A0%9C%EA%B3%B5%ED%95%AD&sls=5
#VIP: https://carmore.kr/home/page-car-list-depth2.html?areaCode=Q_1&rentStartDate=2025-11-05%2010%3A00%3A00&rentEndDate=2025-11-06%2010%3A00%3A00&nationalCode=KR&cii=733&isOverSeas=false&army=0&fishing=0&foreigner=0&pet=0&v=20251104153959
