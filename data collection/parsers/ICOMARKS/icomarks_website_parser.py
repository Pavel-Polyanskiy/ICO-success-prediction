import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import time
import re
from urllib.parse import unquote
import random
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


accept = 'application/json, text/javascript, */*; q=0.01'

driver_path = '/Users/polyanaboss/Desktop/chromedriver'
options = webdriver.ChromeOptions()
url = 'https://icomarks.com/icos?status=ended'


options.add_argument(f'accept={accept}')


driver = webdriver.Chrome(
    executable_path = driver_path,
    options = options)

driver.maximize_window()

try:
    driver.get(url)
    time.sleep(2)

    cookies = driver.find_element_by_class_name('cc-compliance')
    cookies.click()

    time.sleep(2)
    i = 0
    while True:
        #driver.find_element_by_id('show-more'):
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        find_more_element = driver.find_element_by_id('show-more')
        time.sleep(2)
        find_more_element.click()
        i += 1
        time.sleep(3)
        with open(f'data collection/parsers/icomarks_pages/icomarks_html_{i}.html', 'w') as file:
            file.write(driver.page_source)

        print(i)

except Exception as ex:
        print("Execution is completed")

finally:
    driver.quit()
    driver.close()

time_spent = time.time() - start

print(time_spent)
print(i)