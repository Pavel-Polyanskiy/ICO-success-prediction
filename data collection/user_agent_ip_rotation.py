import random
from selenium import webdriver
import time

with open('/Users/polyanaboss/Desktop/Thesis scripts/data collection/parsers/user_agents.txt', 'r') as file:
    user_agents = file.readlines()

with open('/Users/polyanaboss/Desktop/Thesis scripts/data collection/parsers/ip_adresses.txt', 'r') as file:
    ips = file.readlines()

driver_path = '/Users/polyanaboss/Desktop/chromedriver'
options = webdriver.ChromeOptions()


PROXY = "11.456.448.110:8080"
options.add_argument(f'user-agent={random.choice(user_agents)}')
#options.add_argument('--proxy-server=%s' % PROXY)

driver = webdriver.Chrome(
    executable_path = driver_path,
    options = options)

try:
    driver.get('https://www.google.com')
    time.sleep(5)

except Exception as ex:
    print(ex)

finally:
    driver.quit()
    driver.close()

