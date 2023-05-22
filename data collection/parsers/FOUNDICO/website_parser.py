import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import aiohttp
import asyncio
import random

with open('data collection/parsers/user_agents.txt', 'r') as file:
    user_agents = file.readlines()

urls = [f'https://foundico.com/icos/filter/end_datetime/11-19-2022/type/45/?PAGEN_2={i}' for i in range(1, 58)]

async def read_url(session, url):
    async with session.get(url, headers = {'user-agent':random.choice(user_agents).strip()}) as resp:
        return await resp.read()


async def main(url_list):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in url_list:
            tasks.append(asyncio.create_task(read_url(session, url)))
        html_list = await asyncio.gather(*tasks)
        
    return html_list

result = asyncio.run(main(urls))

for i, page in enumerate(result, 1):
    with open(f'data collection/parsers/FOUNDICO/pages/page_{i}.html', 'wb') as file:
        file.write(page)

#######################################################################################################################

names = []
tickers = []
links = []
starts = []
ends = []
ratings = []

for i in range(1, 58):
    with open(f'data collection/parsers/FOUNDICO/pages/page_{i}.html', 'r') as file:
        html = file.read()
        soup = BeautifulSoup(html)
        rows = soup.find('tbody').find_all('tr')

        for row in rows:
            
            try: link = 'https://foundico.com/' + row.find('div', class_ = 'ics-txt').find('a')['href']
            except: link = None
            try: name = row.find('div', class_ = 'ics-txt').find('span', class_ = 'ic-nm').text.split('(')[0].strip()
            except: name = None
            try: ticker = row.find('div', class_ = 'ics-txt').find('span', class_ = 'ic-nm').text.split('(')[1].strip()[:-1]
            except: ticker = None 
            try: start = row.find_all('td')[1].text.strip().split(':')[-1].strip()
            except: start = None
            try: end = row.find_all('td')[2].text.strip().split(':')[-1].strip()
            except: end = None
            try: rating = row.find('b').text
            except: rating = None

            names.append(name)
            tickers.append(ticker)
            links.append(link)
            starts.append(start)
            ends.append(end)
            ratings.append(rating)

        print(f'[INFO]: {i} is parsed')

df = pd.DataFrame({
    'name': names,
    'ticker': tickers,
    'link': links,
    'start': starts,
    'end': ends,
    'rating': ratings
})

df.to_csv('data collection/parsers/FOUNDICO/data_aggregated.csv')


####################################################################################################

urls = df['link'].values.tolist()

result = asyncio.run(main(urls))

for i, page in enumerate(result, 1):
    with open(f'data collection/parsers/FOUNDICO/profile_pages/profile_{i}.html', 'wb') as file:
        file.write(page)


        