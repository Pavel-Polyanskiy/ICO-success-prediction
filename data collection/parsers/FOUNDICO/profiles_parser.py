import os
import pandas as pd
import json
from bs4 import BeautifulSoup

files = os.listdir('data collection/parsers/FOUNDICO/profile_pages')

total_data = []

for i in range(2, len(files)):
    with open(f'data collection/parsers/FOUNDICO/profile_pages/profile_{i}.html', 'r') as file:
        html = file.read()
        soup = BeautifulSoup(html)

        intro_data = {}
        try: name = soup.find('h1').text.split('(')[0].strip()
        except: name = None
        try: ticker = soup.find('h1').text.split('(')[-1][:-1]
        except: ticker = None

        intro_data['name'] = name
        intro_data['ticker'] = ticker
        try:
            ratings = soup.find('div', itemtype = 'http://schema.org/Organization').find_all('div', class_ = 'fl-mrk-item mdl-shadow--2dp')
            for rating in ratings:
                field = rating.text.strip().split(":")[0].strip()
                rating = rating.text.strip().split(":")[1].strip()

                intro_data[field] = rating
        except: 
            intro_data['rating'] = None

        summary = {}
        try:
            table = soup.find('table', class_ = 'smry-table').find_all('tr') 
            for row in table:
                if row.text.strip().split(':')[0] in [ 'Website', 'White paper']:
                    property = row.text.strip().split(':')[0]
                    value = 'https://foundico.com/' + row.find('a')['href']

                elif row.text.strip().split(':')[0] == 'Links':
                    property = row.text.strip().split(':')[0]
                    value = [value['href'] for value in row.find_all('a')]
                else:
                    property = row.text.strip().split(':')[0]
                    value = row.text.strip().split(':')[-1].strip()

                summary[property] = value
        except:
            summary['information'] = None

        try: raised = soup.find('div', id = 'statisticsbox').find('span', class_ = 'clt-ttl').text
        except: raised = None

        try: 
            team = soup.find('section', id = 'ico-team-cont').find('div', class_ = 'row').find_all('div', class_ = 'col-xs-6 col-sm-6 col-md-4 col-lg-4')
            team_links = {}
            for team_member in team:
                name = team_member.text.strip().split('\n')[0]
                links = [value['href'] for value in team_member.find('span', class_ = 'smry-links').find_all('a')]
            team_links[name] = links
        except:
            team_links['team_links'] = None

        profile = {
            'intro': intro_data,
            'summary': summary,
            'raised' : raised,
            'team': team_links
        }

        total_data.append(profile)
        print(f'[INFO]: {i} is parsed')

with open('data collection/parsers/FOUNDICO/json_data_foundico.json', 'w') as file:
    json.dump(total_data, file, indent = 4, ensure_ascii = False)

########################################################################################################################
