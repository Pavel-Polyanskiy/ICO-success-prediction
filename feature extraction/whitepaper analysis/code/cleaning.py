import pandas as pd
import re
import nltk
from nltk.stem import WordNetLemmatizer 
import spacy
from nltk.corpus import stopwords
import time
from pprint import pprint
import concurrent.futures
import numpy as np
import wordninja
from spellchecker import SpellChecker
import enchant

stop_words = stopwords.words('english')
nlp = spacy.load("en_core_web_sm")


df = pd.read_csv('feature extraction/whitepaper analysis/data/wp_text_metadata.csv')



def clean_text(text):
    #basic
    text = text.lower()
    text = re.sub(' +', ' ', text)
    text = re.sub('\n', '', text)
    text = re.sub(r"(http?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)","", text)
    text = re.sub(r"(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)","", text)
    text = re.sub('[^a-zA-Z]+\s*', ' ', text)


    #lemmatization
    doc = nlp(text)
    text = " ".join([token.lemma_ for token in doc])

    #stopwords removal 

    text = [word for word in text.split(' ') if word not in stop_words]
    text = ' '.join(text)

    return text
    
df['text_cleaned'] = None

for i, text in enumerate(df['text'].values):
    text_cleaned = clean_text(text)
    df['text_cleaned'][i] = text_cleaned

    print(f"[INFO]: {i} is done!")


def remove_nonletters(text):
    # Remove all characters that are not letters or whitespace
    cleaned_text = re.sub('[^a-zA-Z\s]', '', text)
    return cleaned_text

df['text'] = df['text'].astype("str").apply(remove_nonletters)
df['text'] = df['text'].apply(lambda text: re.sub(r'\b\w\b', '', text))
df['text'] = df['text'].apply(lambda text: re.sub(r'\b\w{2}\b', '', text))
df['text'] = df['text'].apply(lambda text: text.replace('\t', ''))
df.drop('text', axis = 1, inplace = True)
df.rename(columns = {'text_cleaned': 'text'}, inplace = True)



df.to_csv('feature extraction/whitepaper analysis/data/wp_text_cleaned.csv', index = None)

### 2 step
df = pd.read_csv('feature extraction/whitepaper analysis/data/wp_text_cleaned.csv')


spell = SpellChecker(language = 'en')
spellchecker = enchant.Dict("en_US")

#df['text_new'] = None
#df.drop("text_new", axis = 1, inplace = True)

def correct_spelling(i, text):
    words = ''.join(text.split())
    new_words  = ' '.join(wordninja.split(words))
    misspelled = list(spell.unknown(new_words.split()))
    misspelled = [val for val in misspelled if len(val) > 3 or val in ['api', 'sql']]
    for word in misspelled:
        correction = spellchecker.suggest(word)[0].lower()
        correction = re.sub('[^a-zA-Z\s]', '', correction)
        new_words = new_words.replace(word, correction)
        new_words = re.sub(r'\b\w\b', '', new_words)
        new_words = re.sub(r'\b\w{2}\b', '', new_words)

    df['text'][i] = new_words
    print(f"[INFO]: {i} is done!")

    return new_words


with concurrent.futures.ThreadPoolExecutor() as executor:
    for i, text in enumerate(df['text'].values):
        executor.submit(correct_spelling, i, text)


df.to_csv('feature extraction/whitepaper analysis/data/wp_text_cleaned_v2.csv', index = None)


