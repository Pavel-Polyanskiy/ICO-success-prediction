import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import warnings
import time
import random
from googleapiclient.discovery import build

warnings.filterwarnings('ignore')
df = pd.read_excel('feature extraction/control variables extraction/controls_df.xlsx')
df.info()
df.start = df.start.apply(lambda date: date.strftime('%Y-%m-%d'))
df.end = df.end.apply(lambda date: str(date)[:10])



##### competitors
competitors = []

for i, row1 in df.iterrows():
    count = 0
    for j, row2 in df.iterrows():
        if (row2['start'] >= row1['start'] and row2['start'] <= row1['end']) or \
        (row2['end'] >= row1['start'] and row2['end'] <= row1['end']):
            count += 1
    competitors.append(count - 1)

df['competitors'] = [val + 1 for val in competitors]

df[df['competitors'] == 1025]

########## BTC volatility

btc = pd.read_csv('data processing/historical_data/btc_historical_data.csv')

df.shape

df['btc_volatility'] = np.NaN
for i in range(len(df)):
    btc_prices = btc[(btc["timestamp"] >= df.start[i]) & (btc["timestamp"] <= df.end[i])]['Close']
    btc_volatility = btc_prices.std() / btc_prices.mean()

    df['btc_volatility'][i] = btc_volatility


df[['name', 'start', 'end', 'btc_volatility']].info()

df['btc_volatility'].fillna(df['btc_volatility'].mean(), inplace = True)

#########

media_links = [eval(val) for val in df.media.dropna().values.tolist()]

media_links = [item for sublist in media_links for item in sublist]

len([val for val in media_links if "twitter" in val])

re.search(r'[a-zA-Z]', '3 519 members, 84 online').start()
'3 519 members, 84 online'[:6]

#### Telegram
df.shape
df['tg_followers'] = np.NaN
for i in range(len(df)):
    try:
        links = eval(df['media'][i])
        tg_link = [link for link in links if "t.me" in link][0]
        html = requests.get(tg_link)
        soup = BeautifulSoup(html.content)
        text = soup.find("div", class_ = 'tgme_page_extra').text.strip() 
        letter_index = re.search(r'[a-zA-Z]', text).start()
        n_followers = int(text[:letter_index].strip().replace(' ', ''))
        print(f"{tg_link} is converted to {n_followers}")
        df['tg_followers'][i] = n_followers

    except:
        df['tg_followers'][i] = 0




######### twitter
import tweepy
API_KEY = 'tMJT7FnBScFEmZO462Srsdovv'
API_SECRET  ='YIvXuwxGSEXehg9lMRLckIG5mzybklB3znzNiTJQAFt6v90jOS'
BEARER = 'AAAAAAAAAAAAAAAAAAAAAGOBZAEAAAAAcJL8LpoyYGVm%2FIm5AgSMp25awNo%3DBBPCBnzgYK8w7E7LIaLjwCgPAPOq6FByYejDmnEqtRaLdsMYfJ'
ACCESS_TOKEN = '1492169279843840004-QDfjxsAxWnIV2dCbAophLUoH87nlwk'
ACCESS_SECRET = 'Dv1ZpTpVhp38Mc0IVAb8ogoFr9Nomh0IGq1hFbayyiLJC'
auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

df['twitter_followers'] = np.NaN
df['twitter_following'] = np.NaN
df['tweet_count'] = np.NaN

headers = {
    'Authorization': f'Bearer {BEARER}',
}


for i in range(len(df)):
    try:
        links = eval(df['media'][i])
        twitter_link = [link for link in links if "twitter" in link][0]
        username = twitter_link.split('/')[-1]
        response = requests.get(
        f"https://api.twitter.com/2/users/by/username/{username}?user.fields=public_metrics",
        headers=headers
                )
        response = eval(response.content.decode())
        df['twitter_followers'][i] = response['data']['public_metrics']['followers_count']
        df['twitter_following'][i] = response['data']['public_metrics']['following_count']
        df['tweet_count'][i] = response['data']['public_metrics']['tweet_count']

        print(df['twitter_followers'][i])
        print(f"[INFO]: {i} is done!")
        print("######################")
    except:
        df['twitter_followers'][i] = 0
        df['twitter_following'][i] = 0
        df['tweet_count'][i] = 0


df[['name', 'tweet_count', 'twitter_followers', 'twitter_following']].head(20)




#### youtube
  
youtube = build('youtube', 'v3', 
                        developerKey='AIzaSyA-bi7Yf-1tep6Z4VqRFR9gDIFcbaPr2-4')

df['youtube_followers'] = np.NaN
df['youtube_video_count'] = np.NaN
df['youtube_view_count'] = np.NaN


for i in range(len(df)):
    try:
        links = eval(df['media'][i])
        youtube_link = [link for link in links if "youtube" in link][0]
        id = youtube_link.split('/')[-1]
        ch_request = youtube.channels().list(
            part='statistics',
            id = id) 
        ch_response = ch_request.execute()
        
        sub = ch_response['items'][0]['statistics']['subscriberCount']
        vid = ch_response['items'][0]['statistics']['videoCount']
        views = ch_response['items'][0]['statistics']['viewCount']

        df['youtube_followers'][i] = sub
        df['youtube_video_count'][i] = vid
        df['youtube_view_count'][i] = views

    except:
        df['youtube_followers'][i] = 0
        df['youtube_video_count'][i] = 0
        df['youtube_view_count'][i] = 0

    finally:
        print(f"[INFO]:{i} is done!")


df.columns

df[['name','youtube_followers']]


####

"""
and queried "startup name
+ founder name" for 1-year trend. The name of startups is often duplicated by other companies or could be
very simple words, so we put founder name together to get accurate results.


we determine the Google Search Trends2 of
the terms “Initial Coin Offering”, “cryptocurrency” and
“blockchain” starting from 2017.
"""


from pytrends.request import TrendReq

pytrends = TrendReq(hl='en-US', tz=360)


df.to_csv('feature extraction/control variables extraction/controls.csv', index = None)
"""
twitter  1084
facebook 979
tg 986
medium 821
"""
