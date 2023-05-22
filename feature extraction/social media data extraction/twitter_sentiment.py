import pandas as pd
import re
import nltk
from nltk.stem import WordNetLemmatizer 
from nltk.corpus import stopwords
from transformers import pipeline

names = pd.read_csv('feature extraction/social media data extraction/ticker2name.csv')

tweets_df = pd.read_excel('feature extraction/social media data extraction/twitter_data_v1.xlsx')


sample = tweets_df.iloc[:100, 1:]


sample.to_csv('tweets_sample.csv', index = None)


tickers = list(set(tweets_df['ticker'].values.tolist()))
len(tickers)


lemmatizer = WordNetLemmatizer()
stop_words = stopwords.words('english')

### cleaning
def clean_text(text):
    #basic
    text = text.lower()
    text = re.sub(' +', ' ', text)
    text = re.sub('\n', '', text)
    text = re.sub(r"(http?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)","", text)
    text = re.sub(r"(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)","", text)
    text = re.sub('[^a-zA-Z]+\s*', ' ', text)


    #lemmatization
    
    
    text = " ".join([lemmatizer.lemmatize(word) for word in text.split()])

    #stopwords removal 

    text = [word for word in text.split(' ') if word not in stop_words]
    text = ' '.join(text)

    return text
    



for i, text in enumerate(sample['tweet'].values):
    text_cleaned = clean_text(text)
    sample['tweet'][i] = text_cleaned

    print(f"[INFO]: {i} is done!")



####reformating


grouped = sample.groupby('ticker')
sample = pd.DataFrame(index=grouped.groups.keys())
for ticker, group in grouped:
    tweets = group['tweet'].values
    for i in range(len(tweets)):
        sample.loc[ticker, f'tweet_{i+1}'] = tweets[i]


sample.shape







