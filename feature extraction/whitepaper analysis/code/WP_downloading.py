import urllib.request
import pandas as pd
import numpy as np
import os
import requests
import random
import time
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import PyPDF2

df = pd.read_csv('data collection/final_data.csv')
df.drop('Unnamed: 0', axis = 1, inplace = True)

whitepaper_links = df['whitepaper'].values.tolist()

with open('data collection/parsers/user_agents.txt', 'r') as file:
    user_agents = file.readlines()


def check_validity(file_path):
    try:
        pdf = PyPDF2.PdfFileReader(open(file_path, "rb"))
        status = 'YES'
    except:
        status = 'NO'
        if os.path.isfile(file_path):
            os.remove(file_path)

    return status

len(whitepaper_links)


wp_status = []
for i, link in enumerate(whitepaper_links[7000:], 7000):
    try:
        file_name = f"whitepaper analysis/WP_files/{df['name'][i]}.pdf"
        print(file_name)
        if os.path.isfile(file_name):
            continue
        else:  
            with open(file_name, 'wb') as outfile:
                ua = UserAgent()
                a = ua.random
                user_agent = ua.random
                headers = {'User-Agent': user_agent}
                outfile.write(requests.get(link, headers = headers).content)

        status = check_validity(file_name)
        wp_status.append(status)
    except Exception:
        status = check_validity(file_name)
        wp_status.append(status)




len(os.listdir('whitepaper analysis/WP_files'))



df.head()
df['whitepaper_downloaded'] = np.NaN

for file in os.listdir('whitepaper analysis/WP_files'):
    file_name = file[:-4]
    try:
        df['whitepaper_downloaded'][df['name'] == file_name] = 'YES'

    except:
        continue

df[df['whitepaper_downloaded'] == 'YES']


df['whitepaper_downloaded'].fillna('NO', inplace = True)

df.to_csv('whitepaper analysis/wp_final_df.csv')



df['whitelist_kyc'].replace('Whitelist + KYC', 'yes', inplace = True)
df['whitelist_kyc'].replace('Whitelist', 'yes', inplace = True)
df['whitelist_kyc'].replace('KYC', 'yes', inplace = True)


df['platform'].replace('1', 'Ethereum', inplace = True)

df.info()

df[['soft_cap', 'raised']].dropna()