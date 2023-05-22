import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time

with open('data collection/parsers/icomarks_pages/icomarks_html_250.html', 'r') as file:
    html = file.read()

soup = BeautifulSoup(html)

items = soup.find_all('div', class_ = 'icoListItem')

items_sample = items[:100]

links = []
names = []
starts = []
ends = []
total_ratings = []

for item in items:
    link = 'https://icomarks.com/' + item.find('a', class_ = 'icoListItem__title')['href']
    name = link.split('/')[-1].replace('-', ' ')
    start = item.find('div', class_ = 'icoListItem__start').text.strip()[5:].strip()
    end = item.find('div', class_ = 'icoListItem__end').text.strip()[3:].strip()
    total_rating = item.find('div', class_ = 'icoListItem__rate').text.strip()[:-18].strip()

    links.append(link)
    names.append(name)
    starts.append(start)
    ends.append(end)
    total_ratings.append(total_rating)

df = pd.DataFrame({'name': names,
              'link': links,
              'start': starts,
              'end': ends,
              'total_rating': total_ratings})

df['total_rating'] = df['total_rating'].apply(lambda value: float(value))

df.to_csv('data collection/parsers/icomarks_aggregated.csv')

df.tail(10)