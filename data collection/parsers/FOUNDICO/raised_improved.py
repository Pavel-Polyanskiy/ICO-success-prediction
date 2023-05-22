import os
import pandas as pd
import json
from bs4 import BeautifulSoup

files = os.listdir('data collection/parsers/FOUNDICO/profile_pages')

total_data = []

names = []
raised_data = []


for i in range(2,len(files)): # len(files)
    with open(f'data collection/parsers/FOUNDICO/profile_pages/profile_{i}.html', 'r') as file:
        html = file.read()
        soup = BeautifulSoup(html)
        try: 
            name = soup.find('h1').text.split('(')[0].strip()
            raised = soup.find('div', id = 'statisticsbox').find_all('span', class_ = 'clt-ttl')[1].text
            
        except: 
            name = None
            raised = None
        finally:    
            names.append(name)
            raised_data.append(raised)
            print(f'[INFO]: {i} is parsed')




df = pd.DataFrame({
    'name': names,
    'raised': raised_data
})

df.dropna(how='any', inplace = True)
df.to_csv('data collection/parsers/FOUNDICO/foundico_improved_raised.csv')
########################################################################################################################
