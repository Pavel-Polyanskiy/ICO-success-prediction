import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import json 
import asyncio
import aiohttp
import time
import numpy as np

df = pd.read_csv('data collection/parsers/icomarks_aggregated.csv')
df.drop('Unnamed: 0', axis = 1, inplace = True)

urls = df.link.values.tolist()

################################################################################################################

def intro_data(soup):
    try: name = soup.find('h1', itemprop = 'name').text 
    except: name = None

    try: link = soup.find('a', class_ = 'visitSite')['href']
    except: link = None
    try: ratings = soup.find_all('div', class_ = 'ico-rating__item')
    except: ratings = None
    try: profile_rating = ratings[0].find('div', class_ = 'ico-rating__circle').text.strip()
    except: profile_rating = None
    try: social_activity_rating = ratings[1].find('div', class_ = 'ico-rating__circle').text.strip()
    except: social_activity_rating = None
    try: team_proof_rating = ratings[2].find('div', class_ = 'ico-rating__circle').text.strip() 
    except: team_proof_rating = None

    intro_data = {
        'name': name,
        'link': link,
        'profile_rating': profile_rating,
        'social_activity_rating': social_activity_rating,
        'team_proof_rating': team_proof_rating
    }

    return intro_data


### STATS
def stats(soup):
    stats = soup.find_all('div', class_ = 'icoinfo-block') # 0 - general, 1 - token info
    return stats

#### general
def general_data(stats):
    general = stats[0].find_all('div', class_ = 'icoinfo-block__item')

    general_data = {}
    if len(general) > 0:
        for item in general:
            if item.text.strip().split('\n')[-1] == "Visit" or item.text.strip().split('\n')[-1] == "Read":
                general_data[item.text.strip().split('\n')[0]]  = item.find('a')['href']
            else:
                general_data[item.text.strip().split('\n')[0]] = item.text.strip().split('\n')[1]
    else:
        general_data['general_data'] = None

    return general_data


#### token info

def token_data(stats):
    token_info = stats[1].find_all('div', class_ = 'icoinfo-block__item')

    token_data = {}
    if len(token_info) > 0:
        for token in token_info:
            token_data[token.text.strip().split('\n')[0]] = token.text.strip().split('\n')[1]
    else:
        token_data['token_data'] = None
    return token_data

### FINANCIALS

def financial_data(stats):
    finanials = stats[2].find_all('div', class_ = 'icoinfo-block__item')

    financial_data = {}
    if len(finanials) > 0:
        for financial in finanials:
            financial_data[financial.text.strip().split('\n')[0]] = financial.text.strip().split('\n')[1]
    else:
        financial_data['financial_data'] = None

    return financial_data
### SOCIAL MEDIA

def media_data(stats):
    media = stats[3].find_all('div', class_ = 'icoinfo-block__item')
    media_links = []
    if len(media) > 0:
        for platform in media:
            media_links.append(platform.find('a')['href'])
    else:
        media_links = [None]

    return media_links
    
### TEAM
def team_data(soup):
    team = soup.find_all('div', class_ = 'company-team__item')
    team_links = {}
    if len(team) > 0:
        for team_member in team:
            try: name = team_member.find('div', class_ = 'company-team__title').text.strip()
            except: name = None
            try: link = team_member.find('div', class_ = 'company-team__links').find('a')['href']
            except: link = None
            team_links[name] = link
    else:
        team_links['team_links'] = None

    return team_links

################################################################################################################
total_data = []
file_names = os.listdir('data collection/parsers/icomarks_profile_pages') #icomarks_profile_pages
for i in range(1, len(file_names)): #from 1
    with open(f'data collection/parsers/icomarks_profile_pages/profile_{i}.html', 'r') as file: #icomarks_profile_pages
        html = file.read()
        soup = BeautifulSoup(html)  

        intro = intro_data(soup)
        statistics = stats(soup)
        general = general_data(statistics)
        token = token_data(statistics)
        finance = financial_data(statistics)
        media = media_data(statistics)
        team = team_data(soup)

        profile = {
            'intro': intro,
            'general': general,
            'token': token,
            'finance': finance,
            'media': media,
            'team': team
        }

        total_data.append(profile)
        print(f'[INFO]: #{i} is parsed')




with open('data collection/parsers/json_data.json', 'w') as file:
    json.dump(total_data, file, indent = 4, ensure_ascii = False)

################################################################################################################