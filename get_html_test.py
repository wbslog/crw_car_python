import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By


browser = webdriver.Chrome()
browser.maximize_window()

url='https://finance.naver.com/sise/sise_market_sum.naver?&page='

browser.get(url)



