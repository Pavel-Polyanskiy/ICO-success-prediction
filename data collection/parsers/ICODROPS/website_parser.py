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
import pandas as pd


driver_path = '/Users/polyanaboss/Desktop/chromedriver'
options = webdriver.ChromeOptions()
url = 'https://icodrops.com/category/ended-ico/'




driver = webdriver.Chrome(
    executable_path = driver_path,
    options = options)

driver.maximize_window()

try:
    driver.get(url)
    time.sleep(2)
    i = 0
    while True:
        #driver.find_element_by_id('show-more'):
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(6)
        i += 1
        with open(f'data collection/parsers/ICODROPS/icodrops_files/icodrops_html_{i}.html', 'w') as file:
            file.write(driver.page_source)

except Exception as ex:
        print("Execution is completed")

finally:
    driver.quit()
    driver.close()


with open('data collection/parsers/ICODROPS/icodrops_files/icodrops_html_7.html', 'r') as file:
    data = file.read()

soup = BeautifulSoup(data)

icos = soup.find_all('div', class_ = 'col-md-12 col-12 a_ico')

names = []
raised_data = []
links = []
dates = []
tickers = []
for i, ico in enumerate(icos, 1):
    try:
        name = ico.find('h3').text.strip()
        link = ico.find('h3').find('a')['href']
        raised = ico.find('span', class_ = 'green').text.strip()
        date = ico.find('div', class_ = 'date').text.strip()
        ticker = ico.find('div', class_ = 'meta_icon').text.strip()
        names.append(name)
        links.append(link)
        raised_data.append(raised)
        dates.append(date)
        tickers.append(ticker)
    except:
        continue

    print(f'[INFO]: {i} is parsed')

df = pd.DataFrame({
    'name': names,
    'link': links,
    'raised': raised_data,
    'date': dates,
    'ticker': tickers
})

df.ticker = df.ticker.apply(lambda text: text[7:].strip())
df['name'] = df['name'].apply(lambda value: value.lower())

df.date = df.date.apply(lambda date: date[-4:])

df = df[df['date'] >= "2022"]



final_df = pd.read_csv('whitepaper analysis/wp_final_df.csv')

len([name for name in df['name'].values if name in final_df['name'].values])


df.to_csv('data collection/parsers/ICODROPS/icodrops_2023.csv', index = None)

len(df)