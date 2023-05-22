import asyncio
import aiohttp
import time
import numpy as np
import pandas as pd
import random
import os

with open('data collection/parsers/user_agents.txt', 'r') as file:
    user_agents = file.readlines()

with open('data collection/parsers/ip_adresses.txt', 'r') as file:
    ip_adresses = file.readlines()

df = pd.read_csv('data collection/parsers/icomarks_aggregated.csv')
df.drop('Unnamed: 0', axis = 1, inplace = True)

urls = df.link.values.tolist()





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

for i in range(201, len(urls), 100):
    if i == 5001:
        results = asyncio.run(main(urls[i:]))
    
    else:
        results = asyncio.run(main(urls[i:i + 100]))
    
    print(f'[INFO]: #{i}-{i+100} are parsed')
    for k in range(100):
        with open(f'data collection/parsers/icomarks_profile_pages/profile_{k + i}.html', 'wb') as file:
            file.write(results[k])

    length = len(os.listdir('data collection/parsers/icomarks_profile_pages'))
    print(f'[INFO]: there are {length} files!')

    time.sleep(2)


