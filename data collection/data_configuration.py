import pandas as pd
import numpy as np
import json


############################################################################################################

# ICOMARKS
with open('data collection/parsers/ICOMARKS/json_data_icomarks.json', 'r') as file:
    data = json.load(file, strict=False)

variables = ['name', 'link', 'profile_rating', 'social_activity_rating', 'team_proof_rating', 'White paper:', 'ICO Time:', 'Country:',  "Ticker:",
'Whitelist/KYC:', 'Platform:', 'Token Type:', 'ICO Price:', 'Accepting:', 'Soft cap:', 'Hard cap:', 'media', 'team', 'Bonuses:','Available for sale:', 'Raised']

len(variables)
sample = data[:100]
keys = ['intro', 'general', 'token', 'finance', 'team', 'media']

total_data = []

for ico in data:
    ico_dict = {}
    for key in keys[:-2]:
        ico_dict.update(ico[key])
        ico_dict['media'] = ico['media']
        ico_dict['team'] = ico['team']
    ico_full = dict.fromkeys(variables, None)
    ico_full.update(ico_dict)

    total_data.append(ico_full)

final_df = pd.DataFrame(total_data)

final_df = final_df[variables]
final_df.to_csv('data collection/icomarks_final.csv')
final_df.shape

icomarks_agg = pd.read_csv('data collection/parsers/ICOMARKS/icomarks_aggregated.csv')


############################################################################################################

# FOUNDICO
with open('data collection/parsers/FOUNDICO/json_data_foundico.json', 'r') as file:
    data = json.load(file, strict=False)
#kyc + 1, ratings
variables = ['name', 'ticker', 'Main info', 'Finance', 'Product', 'Team', 'Marketing', 
'Type', 'Whitelist of investors', 'KYC of investors', 'Goal of funding (Soft cap)', 'Goal of funding (Hard cap)',
'Tokens for sale', 'Token price', 'Bounty program', 'White paper', 'Currencies', 'Platform', 'Location', 'Website',
'Links', 'raised', 'team']

keys = ['intro', 'summary', 'raised', 'team']

total_data = []

for ico in data:
    ico_dict = {}
    for key in keys[:-2]:
        ico_dict.update(ico[key])
        ico_dict['raised'] = ico['raised']
        ico_dict['team'] = ico['team']
    ico_full = dict.fromkeys(variables, None)
    ico_full.update(ico_dict)

    total_data.append(ico_full)

final_df = pd.DataFrame(total_data)

final_df = final_df[variables]

foundico = pd.read_csv('data collection/parsers/FOUNDICO/data_aggregated.csv')
foundico.columns
final_df = final_df.merge(foundico[['name', 'start', 'end', 'rating']])
final_df.to_csv('data collection/foundico_final.csv')

############################################################################################################

#TORD

tord = pd.read_csv('data collection/parsers/tord_v3.csv', engine='python', sep = ';')
tord.info()

variables = ['name', 'token', 'country', 'ico_start', 'ico_end', 'price_usd', 'raised_usd', 'token_for_sale', 'whitelist', 'kyc',
'bonus', 'platform', 'accepting', 'link_white_paper', 'linkedin_link', 'website', 'rating', 'ERC20']

len(variables)

tord = tord[variables]

tord.to_csv('data collection/tord_final.csv')

############################################################################################################

#TOTAL CONCATENATION

icomarks = pd.read_csv('data collection/icomarks_final.csv')
icomarks.drop('Unnamed: 0', axis = 1, inplace = True)
icomarks.columns

icomarks['start'] = icomarks['ICO Time:'].apply(lambda value: value.split(' - ')[0].strip() if type(value) == str else None)
icomarks['end'] = icomarks['ICO Time:'].apply(lambda value: value.split(' - ')[1].strip() if type(value) == str else None)

col_names = {
    'White paper:': 'whitepaper',
    'Country:': 'country',
    'Whitelist/KYC:': 'whitelist_kyc',
    'Platform:': 'platform', 
    'Token Type:': 'token_type', 
    'ICO Price:': 'ico_price',
    "Ticker:": 'ticker',
    'Accepting:': 'accepted_currencies',
    'Soft cap:': 'soft_cap', 
    'Hard cap:': 'hard_cap',
    'Bonuses:': 'bonus',
    'Available for sale:': 'available_for_sale',
    'Raised': 'raised'
}
icomarks.rename(columns = col_names, inplace = True)
icomarks.drop('ICO Time:', axis = 1, inplace = True)
icomarks.columns

icomarks['bonus'].fillna('no', inplace = True)
icomarks['bonus'] = icomarks['bonus'].apply(lambda value: 'yes' if value != 'no' else 'no')
icomarks['profile_rating'] = pd.to_numeric(icomarks['profile_rating'],errors='coerce')

icomarks_rating = []
for i in range(len(icomarks)):
    rating = 0.45 * icomarks['profile_rating'][i] + 0.35 * icomarks['social_activity_rating'][i] + 0.2 * icomarks['team_proof_rating'][i]
    rating = np.round(rating, 1)
    icomarks_rating.append(rating)

icomarks['rating'] = icomarks_rating



foundico = pd.read_csv('data collection/foundico_final.csv')
foundico.drop('Unnamed: 0', axis = 1, inplace = True)
foundico.columns
whitelist_kyc = []
for i in range(len(foundico)):
    if foundico['KYC of investors'][i] == 'Yes' or  foundico['Whitelist of investors'][i] == 'Yes':
        whitelist_kyc.append('yes')
    else:
        whitelist_kyc.append('no')

bonus = []
for i in range(len(foundico)):
    if foundico['Bounty program'][i] == 'Yes':
        bonus.append('yes')
    else:
        bonus.append('no')

#profile_rating,social_activity_rating,team_proof_rating
foundico['whitelist_kyc'] = whitelist_kyc
foundico['bonus'] = bonus
col_names_2 = {
    'Website':'link',
    'Main info': 'profile_rating',
    'Team': 'team_proof_rating',
    'Marketing': 'social_activity_rating',
    'White paper': 'whitepaper',
    'Location': 'country',
    'Platform': 'platform', 
    'Type': 'token_type', 
    'Token price': 'ico_price',
    "Ticker:": 'ticker',
    'Currencies': 'accepted_currencies',
    'Goal of funding (Soft cap)': 'soft_cap', 
    'Goal of funding (Hard cap)': 'hard_cap',
    'Bonuses:': 'bonus',
    'Tokens for sale': 'available_for_sale',
    'Links': 'media'
}

foundico.rename(columns = col_names_2, inplace = True)

[col for col in icomarks.columns if col not in foundico.columns]

tord.columns

whitelist_kyc_2 = []
for i in range(len(tord)):
    if tord['whitelist'][i] == 'Yes' or  tord['kyc'][i] == 'Yes':
        whitelist_kyc_2.append('yes')
    else:
        whitelist_kyc_2.append('no')
tord['whitelist_kyc'] = whitelist_kyc_2

col_names_3 = {
    'website':'link',
    'link_white_paper': 'whitepaper', 
    'price_usd': 'ico_price',
    "token:": 'ticker',
    'accepting': 'accepted_currencies',
    'token_for_sale': 'available_for_sale',
    'linkedin_link': 'media',
    'ico_start': 'start',
    'ico_end': 'end',
    'raised_usd': 'raised'
}

tord.rename(columns = col_names_3, inplace = True)

tord.rename(columns = {'token': 'ticker'}, inplace = True)

tord.rating.value_counts()

month2number = {
    'Jan': '1',
    'Feb': '2',
    'Mar': '3',
    'Apr': '4',
    'May': '5',
    'Jun': '6',
    'Jul': '7',
    'Aug': '8',
    'Sep': '9',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12'
}
ratings = []
for rating in tord.rating:
    if len(str(rating)) == 6:
        integer = rating.split('.')[0][1]
        decimals = month2number[rating.split('.')[1]]
        rating_new = '.'.join([integer, decimals])
    
    else:
        rating_new = rating
    
    ratings.append(rating_new)

tord['rating'] = ratings

[col for col in icomarks.columns if col not in tord.columns]

tord['ERC20'].replace(1, 'ERC20', inplace = True)
tord['ERC20'].replace(0, None, inplace = True)
tord.rename(columns = {'ERC20': 'token_type'}, inplace = True)

foundico['ticker'] = foundico['ticker'].apply(lambda value: value.lower())
icomarks['ticker'] = icomarks['ticker'].apply(lambda value: str(value).lower())
tord['ticker'] = tord['ticker'].apply(lambda value: str(value).lower())

icomarks['ticker'] .replace('nan', np.NaN, inplace = True)
tord['ticker'] .replace('nan', np.NaN, inplace = True)


icomarks['ticker'].value_counts()

tord[tord['ticker'] == 'spc']

icomarks['name'] = icomarks['name'].apply(lambda value: value.lower())

foundico['name'] = foundico['name'].apply(lambda value: value.lower())

tord['name'] = tord['name'].apply(lambda value: value.lower())

names = icomarks['name'].values.tolist()  + foundico['name'].values.tolist() + tord['name'].values.tolist()


icomarks[['name', 'ticker', 'link']][icomarks['ticker'] == 'spc']
tord[['name', 'ticker', 'link']][tord['ticker'] == 'spc']
foundico[['name', 'ticker', 'link']][foundico['ticker'] == 'spc']

freq = dict(zip(np.unique(names, return_counts=True)[0], np.unique(names, return_counts=True)[1]))

dict(sorted(freq.items(),
                           key=lambda item: item[1],
                           reverse=True))

foundico = foundico[icomarks.columns]

for col in [col for col in foundico.columns if col not in tord.columns]:
    tord[col] = np.NaN

tord = tord[icomarks.columns]

sample = icomarks.iloc[:5, :]



new_data = []
for i in range(len(icomarks)):
    ico = dict(icomarks.iloc[i, :])
    try:
        icomarks_data = tord[tord['name'] == ico['name']].iloc[0, :].to_dict()
        for key in ico.keys():
            if pd.notnull(ico[key]) == False and pd.notnull(icomarks_data[key]) == True:
                ico[key] = icomarks_data[key]
        new_data.append(ico)
    except:
        new_data.append(ico)

new_df = pd.DataFrame(new_data, columns = icomarks.columns)

def add_data(base_df, new_df):
    new_data = []
    for i in range(len(base_df)):
        ico = dict(base_df.iloc[i, :])
        try:
            icomarks_data = new_df[new_df['name'] == ico['name']].iloc[0, :].to_dict()
            for key in ico.keys():
                if pd.notnull(ico[key]) == False and pd.notnull(icomarks_data[key]) == True:
                    ico[key] = icomarks_data[key]
            new_data.append(ico)
        except:
            new_data.append(ico)

    new_df = pd.DataFrame(new_data, columns = icomarks.columns)
    return new_df

new_df = add_data(icomarks, tord)
new_df_2 = add_data(new_df, foundico)


icomarks.raised.isnull().sum()
tord.raised.isnull().sum()
foundico.raised.isnull().sum()
new_df.raised.isnull().sum()
new_df_2.raised.isnull().sum()

names_foundico = [name for name in foundico['name'].values if name not in new_df_2['name'].values]
names_tord = [name for name in tord['name'].values if name not in new_df_2['name'].values]

indices_foundico = [i for i, name in enumerate(foundico['name'].values) if name in names_foundico]
indices_tord = [i for i, name in enumerate(tord['name'].values) if name in names_tord]

foundico_add = foundico.iloc[indices_foundico]
tord_add = tord.iloc[indices_tord]

total = pd.concat([new_df_2, tord_add, foundico_add], axis = 0).reset_index(drop = True)

len(total) - total['raised'].isnull().sum()

total.to_csv('data collection/final_data.csv')

