import pandas as pd
import re
import numpy as np
import statistics
import pprint
from dateutil import parser
import warnings
from collections import Counter
warnings.filterwarnings('ignore')
eth_prices = pd.read_csv('data processing/historical_data/eth_historical_data.csv')
btc_prices =  pd.read_csv('data processing/historical_data/btc_historical_data.csv')

eur_historical = pd.read_csv('data processing/historical_data/eur_historical_data.csv', sep = ';')[['Date', 'Price']]
eur_historical.Date = eur_historical.Date.apply(lambda date: parser.parse(date).strftime('%Y-%m-%d'))

dates = pd.read_excel('data processing/date_processed.xlsx')
dates.start_processed = dates.start_processed.apply(lambda date: date.strftime('%Y-%m-%d'))
dates.end_processed = dates.end_processed.apply(lambda date: str(date)[:10])


eth_prices[eth_prices['timestamp']=='2017-08-10']['Close'].values[0]




df = pd.read_excel('data processing/tokens_collected.xlsx')
prices = df[df['status'] == 'DONE']['ico_price'].values.tolist()
df = pd.merge(df, dates, on = 'name')
df.drop(['Unnamed: 0', 'start_y', 'end_y'], axis = 1, inplace = True)
df.rename(columns = {
    'start_processed' : 'start',
    'end_processed' : 'end'
}, inplace = True)

df.columns
df.shape
df['ico_price_processed'] = np.NaN

df.drop_duplicates(subset='name', keep='first', inplace=True)

df.reset_index(inplace = True)
df.drop(['level_0', 'index'], inplace = True, axis = 1)

len(prices)
############################### ICO_PRICE
unknown_types = []
correct_prices = {}
for i, price in enumerate(prices):
    try:
        price_processed = float(str(price).strip()) # 0.0129
        correct_prices[price] = price_processed
        df['ico_price_processed'][i] =  price_processed
    except:
        try:
            if "=" in price and ("USD" in price.upper() or "USDT" in price or "$" in price) and "-" not in price and "1 USD" not in price: # 1 AIO = 1.2 USD
                parts = price.split('=')
                usd_part = [value for value in parts if 'USD' in value or 'USDT' in value or '$' in value][0].strip()
                token_part = [value for value in parts if 'USD' not in value and 'USDT' not in value and '$' not in value][0].strip()
                token_part = float(re.sub(r'[^\d]', ' ', token_part).strip().replace(' ', ''))
                usd_part = usd_part.replace('USD', '').strip()
                usd_part = usd_part.replace('T', '').strip()
                usd_part = usd_part.replace('$', '').strip()
                usd_part = usd_part.replace(',', '').strip()
                price_processed = float(usd_part) / token_part
                

            elif "=" in price and ('.1 USD' in price or '0.01 USD' in price or '0.001 USD' in price or '0.0001 USD' in price or '0.00001 USD' in price or '0.000001 USD' in price):
                parts = price.split('=')
                usd_part = [value for value in parts if 'USD' in value or 'USDT' in value or '$' in value][0].strip()
                token_part = [value for value in parts if 'USD' not in value and 'USDT' not in value and '$' not in value][0].strip()
                token_part = float(re.sub(r'[^\d]', ' ', token_part).strip().replace(' ', ''))
                usd_part = usd_part.replace('USD', '').strip()
                usd_part = usd_part.replace('T', '').strip()
                usd_part = usd_part.replace('$', '').strip()
                usd_part = usd_part.replace(',', '').strip()
                price_processed = float(usd_part) / token_part

            elif "=" in price and ("1 USD" in price or "1 USDT" in price or "1$" in price) and '.1 USD' not in price: #474 PAT = 1 USD
                price_parts = price.split(' = ')
                token_part = [part for part in price_parts if "USD" not in part and "USDT" not in part][0].strip()
                price_processed = 1 / float(re.sub(r'[^\d]', ' ', token_part).strip().replace(' ', ''))

            elif "=" in price and '-' in price and 'USD' in price.upper(): # '1 GTA = 0.04 - 0.05 USD'
                price_ = [value for value in price.split('=') if 'USD' in value][0]
                prices_part = price_.strip().split('-')
                price_processed = statistics.mean([float(price.replace('USD', '').strip()) for price in prices_part])

            elif "=" in price and "1 ETH" in price: #1 ETH = 300,000 WDZ
                parts = price.split('=')
                token_part = [value for value in parts if 'ETH' not in value][0].strip()
                token_part = re.sub(r'[^\d]', ' ', token_part).strip().replace(' ', '')
                token_part = float(token_part)
                eth_price = eth_prices[eth_prices['timestamp']== df['start'][i]]['Close'].values[0] 
                price_processed = eth_price / token_part

            elif "=" in price and 'ETH' in price: # 1 XDRAC = 0.00004 ETH
                parts = price.split('=')
                token_part = [value for value in parts if 'ETH' in value][0].strip()
                token_part = token_part.replace('ETH', '').strip()
                token_part = token_part.replace(',', '.').strip()
                token_part = float(token_part)
                eth_price = eth_prices[eth_prices['timestamp']== df['start'][i]]['Close'].values[0] 
                price_processed = eth_price * token_part

            elif "=" in price and "1 BTC" in price: #1 BTC = 300,000 WDZ
                parts = price.split('=')
                token_part = [value for value in parts if 'BTC' not in value][0].strip()
                token_part = re.sub(r'[^\d]', ' ', token_part).strip().replace(' ', '')
                token_part = float(token_part)
                btc_price = eth_prices[btc_prices['timestamp']== df['start'][i]]['Close'].values[0] 
                price_processed = btc_price / token_part

            elif "=" in price and 'BTC' in price: # 1 XDRAC = 0.00004 BTC
                parts = price.split('=')
                token_part = [value for value in parts if 'BTC' in value][0].strip()
                token_part = token_part.replace('BTC', '').strip()
                token_part = token_part.replace(',', '.').strip()
                token_part = float(token_part)
                btc_price = btc_prices[btc_prices['timestamp']== df['start'][i]]['Close'].values[0] 
                price_processed = eth_price * token_part

            elif "USD" in price.upper() or '$' in price:
                price_processed = price.upper().replace('USD', '').strip()
                price_processed = price_processed.replace('$', '').strip()
                price_processed = float(price_processed)

            elif "=" in price and "EUR" in price: # 1 AIO = 1 EUR
                parts = price.split('=')
                usd_part = [value for value in parts if 'EUR' in value][0].strip()
                usd_part = usd_part.replace('EUR', '').strip()
                price_processed = float(usd_part) * 1.1
                

            elif "=" in price and "EUR" in price: # 1 AIO = 1 EUR
                parts = price.split('=')
                usd_part = [value for value in parts if 'EUR' in value][0].strip()
                usd_part = usd_part.replace('EUR', '').strip()
                price_processed = float(usd_part) * 1.1

            elif "=" in price and "GBP" in price: # 1 AIO = 1 EUR
                parts = price.split('=')
                usd_part = [value for value in parts if 'GBP' in value][0].strip()
                usd_part = usd_part.replace('GBP', '').strip()
                price_processed = float(usd_part) * 1.3



            df['ico_price_processed'][i] =  price_processed
            correct_prices[price.strip()] = price_processed

        except Exception as e:
            unknown_types.append(price)
            correct_prices[price] = e
            df['ico_price_processed'][i] = 'ERROR'


correct_prices['1 BID = 0.01 USD']
pprint.pprint(correct_prices)

df['ico_price_processed']
df[['ico_price', 'ico_price_processed']]
len(correct_prices)

#######
unknown_caps = []
caps_corrected = {}
caps = df[df['status'] == 'DONE']['soft_cap'].values.tolist()

len(caps)
df['soft_cap_processed'] = np.NaN
for i, cap_start in enumerate(caps):
    try: 
        cap = int(cap_start)
        df['soft_cap_processed'][i] = cap
    except:
        try:
            if "USD" in cap_start or 'USDT' in cap_start or '$' in cap_start:
                cap = re.sub(r'USDT|USD|$', '', cap_start)
                cap =  cap.replace('$', '')
                cap = re.sub(',', '', cap).strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap)

            elif "EUR" in cap_start:
                cap = cap_start.replace('EUR', '')
                cap = cap.replace(',', '').strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap) * 1.1

            elif "ETH" in cap_start:
                eth_price = eth_prices[eth_prices['timestamp']== df['start'][i]]['Close'].values[0]
                cap = cap_start.replace('ETH', '').strip()
                cap = cap.replace(',', '').strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap) * eth_price

            elif "BTC" in cap_start:
                btc_price = btc_prices[btc_prices['timestamp']== df['start'][i]]['Close'].values[0]
                cap = cap_start.replace('BTC', '').strip()
                cap = cap.replace(',', '').strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap) * btc_price

            else:
                cap = None
                unknown_caps.append(cap_start)
                df['soft_cap_processed'][i] = 'ERROR'
            print(i)
            print(cap)
            print("##########################")
            caps_corrected[cap_start] = cap
            df['soft_cap_processed'][i] = cap
        except Exception as e:
            unknown_caps.append(cap_start)
            caps_corrected[cap_start] = e
            df['soft_cap_processed'][i] = 'ERROR'


####### hard cap
unknown_caps_hard = []
caps_corrected_hard = {}
caps_hard = df[df['status'] == 'DONE']['hard_cap'].values.tolist()
len(caps_hard)

df['hard_cap_processed'] = np.NaN

for i, cap_start in enumerate(caps_hard):
    try: 
        cap = int(cap_start)
        df['hard_cap_processed'][i] = cap
    except:
        try:
            if "USD" in cap_start or 'USDT' in cap_start or '$' in cap_start:
                cap = re.sub(r'USDT|USD|$', '', cap_start)
                cap =  cap.replace('$', '')
                cap = re.sub(',', '', cap).strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap)

            elif "EUR" in cap_start:
                cap = cap_start.replace('EUR', '')
                cap = cap.replace(',', '').strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap) * 1.1

            elif "ETH" in cap_start:
                eth_price = eth_prices[eth_prices['timestamp']== df['start'][i]]['Close'].values[0]
                cap = cap_start.replace('ETH', '').strip()
                cap = cap.replace(',', '').strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap) * eth_price

            elif "BTC" in cap_start:
                btc_price = btc_prices[btc_prices['timestamp']== df['start'][i]]['Close'].values[0]
                cap = cap_start.replace('BTC', '').strip()
                cap = cap.replace(',', '').strip()
                cap = cap.replace('.', '').strip()
                cap = cap.replace(' ', '').strip()
                cap = int(cap) * btc_price

            else:
                cap = None
                unknown_caps_hard.append(cap_start)
                df['hard_cap_processed'][i] = 'ERROR'
            print(i)
            print(cap)
            print("##########################")
            caps_corrected_hard[cap_start] = cap
            df['hard_cap_processed'][i] = cap
            
        except Exception as e:
            unknown_caps_hard.append(cap_start)
            caps_corrected_hard[cap_start] = e
            df['hard_cap_processed'][i] = 'ERROR'


### total supply


unknown_supply = []
supply_corrected = {}
supply = df[df['status'] == 'DONE']['available_for_sale'].values.tolist()

df['total_supply_processed'] = np.NaN

for i, token_number in enumerate(supply):
    try:
        tokens = float(token_number)
        supply_corrected[token_number] = tokens
        df['total_supply_processed'][i] = tokens
    except:
        try:
            letter = re.findall(r'[A-Za-z]', token_number)[0]
            tokens = token_number.split(letter)[0]
            tokens = float(re.sub(r'[^\d]', ' ', tokens).strip().replace(' ', ''))
            supply_corrected[token_number] = tokens
            df['total_supply_processed'][i] = tokens

        except Exception as e:
            unknown_supply.append(token_number)
            supply_corrected[token_number] = e
            tokens = None
            df['total_supply_processed'][i] = 'ERROR'

    finally:
        print(f"{token_number} is converted to {tokens}")
        print("#############################")


#########

unknown_raised = []
raised = df.raised.values.tolist()

wrong_formats = [val for val in df.raised.values.tolist() if str(val)[-3] == '.']
df['raised_processed'] = np.NaN
for i, raised in enumerate(raised):
    if raised in wrong_formats:
        df['raised_processed'][i] = "ERROR"
    try:
        raised_corrected = float(raised)
        df['raised_processed'][i] = raised_corrected
    
    except:
        try:
            raised_corrected = raised.replace('.', '').strip()
            raised_corrected = raised_corrected.replace(',', '').strip()
            raised_corrected = raised_corrected.replace('$', '').strip()
            raised_corrected = raised_corrected.replace(' ', '').strip()
            raised_corrected = raised_corrected.replace('USD', '').strip()
            raised_corrected = float(raised_corrected)
            df['raised_processed'][i] = raised_corrected

        except:
            unknown_raised.append(raised)
            df['raised_processed'][i] = "ERROR"
            raised_corrected = None

        finally:
            print(f"{raised} is converted to {raised_corrected}")



#####
df.to_csv('data processing/numerical_processed.csv', index = None)


pprint.pprint(unknown_types)

##########

df.columns

categorical = df[['name', 'country', 'whitelist_kyc', 'platform', 'token_type', 'accepted_currencies', 'bonus']]

categorical['accepted_currencies']
currency_list = categorical['accepted_currencies'].values.tolist()
categorical['accepted_currencies'].info()

currency_list = ','.join(categorical['accepted_currencies'].dropna()).split(',')
currency_list = [value.strip() for value in currency_list]


counter = Counter(currency_list)

pprint.pprint(counter)
1 - 1095/1108
241 / 1122
213
149 + 65 + 27
crypto_list = ['eth', 'btc', 'ltc', 'fiat', 'n_currencies']
for col in crypto_list:
    categorical[col] = np.NaN

pd.isnull(categorical['accepted_currencies'][1015])

for i in range(len(categorical)):
    string_values = categorical['accepted_currencies'][i]
    if pd.isnull(string_values):
        continue
    categorical['n_currencies'][i] = len(string_values.split(','))
    if 'ETH' in string_values:
        categorical['eth'][i] = 1
    if 'BTC' in string_values:
        categorical['btc'][i] = 1
    if 'LTC' in string_values:
        categorical['ltc'][i] = 1
    if 'fiat' in string_values.lower() or 'eur' in string_values.lower() or 'usd' in string_values.lower() or 'gbp' in string_values.lower():
        categorical['fiat'][i] = 1
    else:
        continue
    

categorical[['eth', 'btc', 'ltc', 'fiat', 'n_currencies', 'bonus']]

categorical['bonus'] = pd.get_dummies(categorical['bonus'], drop_first=True)
categorical['whitelist_kyc'] = pd.get_dummies(categorical['whitelist_kyc'], drop_first=True)

# 'platform'
categorical.platform.fillna('Ethereum', inplace = True)

platforms = [str(value).replace(',', '').strip() for value in categorical.platform.values.tolist()]

categorical['platform_eth'] = categorical['platform'].apply(lambda row: 1 if row == 'Ethereum' else 0)


#'token_type'

categorical.token_type.value_counts()[:10].sum()

categorical.info()

categorical['is_erc20'] = np.NaN
categorical['is_utility_token'] = np.NaN

for i in range(len(categorical)):
    token_type = categorical.token_type[i]
    if pd.isnull(token_type):
        continue

    elif 'ERC' in token_type.upper():
        categorical['is_erc20'][i] = 1
    elif 'utility' in token_type.lower():
        categorical['is_utility_token'][i] = 1
    
    else:
        continue

categorical.accepted_currencies.value_counts()
categorical.columns
categorical['n_currencies'] = categorical['n_currencies'].fillna(categorical['n_currencies'].mean()).astype(int)
binary_cols = ['whitelist_kyc','eth', 'btc', 'ltc', 'fiat', 'platform_eth', 'is_erc20', 'is_utility_token', 'bonus']

categorical.country.fillna('Singapore', inplace = True)

categorical[binary_cols] = categorical[binary_cols].fillna(0).astype('int')

categorical['n_currencies'].fillna(categorical['n_currencies'].mean())

981/1122
categorical.to_csv('data processing/categorical_processed.csv', index = None)
#country

categorical['country'].value_counts()[:10]


num_names = pd.read_csv('data processing/numerical_processed.csv')['name'].values.tolist()

cat_names = categorical.name.values.tolist()

len(cat_names)

[name for name in cat_names if name not in num_names]


########## countries data
from collections import Counter

import os

hc_df = pd.read_csv('data collection/macro indicators/human_capital_indicators.csv', sep = ';')
bi_df = pd.read_excel('data collection/macro indicators/business_indicators.xlsx')
ec_df = pd.read_csv('data collection/macro indicators/economic_indicators.csv', sep = ';')
pol_df = pd.read_excel('data collection/macro indicators/political_indicators.xlsx')


pol_df.columns = ec_df.columns

pd.DataFrame({'original':list(set([val for val in df.country.values if val not in pol_df['Country Name'].values]))}).to_csv('data processing/countries_mapping.csv',index = None)

maps = pd.read_csv('data processing/countries_mapping.txt', sep = '\t')
maps = dict(zip(maps.original.values,maps.correct.values ))

df.country

countries = []
for country in df.country.values:
    try:
        if country in pol_df['Country Name'].values:
            countries.append(country)
        else:
            countries.append(maps[country])
    except:
        countries.append('Unknown')

df['countries_corrected'] = countries
df.columns
df = df[['name', 'countries_corrected', 'start', 'end']]


ec_df.replace('...', np.NaN, inplace = True)
ec_df.replace('..', np.NaN, inplace = True)
ec_df.replace('', np.NaN, inplace = True)
ec_df.replace(' ', np.NaN, inplace = True)
ec_df['Series Name'].value_counts()


ec_df[(ec_df['Country Name'] == "Marshall Islands") & (ec_df['Series Name'] == 'GDP per capita (current US$)')]["2021"].values[0]


################ ECONOMIC

##### GDP
gpd_errors = []
df['gdp_per_capita'] = np.NaN
for i in range(len(df)):
    country = df.countries_corrected[i]
    year = df.start[i][:4]
    try:
        if country == "Unknown" or country == "Worldwide":
            value = None
        elif year == '2022':
            value = ec_df[(ec_df['Country Name'] == country) & (ec_df['Series Name'] == 'GDP per capita (current US$)')]["2021"].values[0]

        elif year == '2016':
            value = ec_df[(ec_df['Country Name'] == country) & (ec_df['Series Name'] == 'GDP per capita (current US$)')]["2017"].values[0]
        
        else: 
            value = ec_df[(ec_df['Country Name'] == country) & (ec_df['Series Name'] == 'GDP per capita (current US$)')][year].values[0] 
        df['gdp_per_capita'][i] = value
    except:
        print(country, year)
        df['gdp_per_capita'][i] = None
        gpd_errors.append(f"{country} - {year}")


##### Unemployment

ec_df['Series Name'].drop_duplicates(keep='first').values.tolist()

indicator = "Unemployment, total (% of total labor force) (national estimate)"
unemployment_errors = []
df['unemployment_rate'] = np.NaN
for i in range(len(df)):
    country = df.countries_corrected[i]
    year = df.start[i][:4]
    try:
        if country == "Unknown" or country == "Worldwide":
            value = None
        elif year == '2022':
            value = ec_df[(ec_df['Country Name'] == country) & (ec_df['Series Name'] == indicator)]["2021"].values[0]

        elif year == '2016':
            value = ec_df[(ec_df['Country Name'] == country) & (ec_df['Series Name'] == indicator)]["2017"].values[0]
        
        else: 
            value = ec_df[(ec_df['Country Name'] == country) & (ec_df['Series Name'] == indicator)][year].values[0] 
        df['unemployment_rate'][i] = value
    except:
        print(country, year)
        df['unemployment_rate'][i] = None
        gpd_errors.append(f"{country} - {year}")


############# human capital


hc_df['Series Name'].drop_duplicates(keep='first').values.tolist()

indicator = "Human capital index (HCI) (scale 0-1)"
hc_score_errors = []
df['human_capital_index'] = np.NaN
for i in range(len(df)):
    country = df.countries_corrected[i]
    year = df.start[i][:4]
    try:
        if country == "Unknown" or country == "Worldwide":
            value = None
        elif year == '2022':
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)]["2021"].values[0]

        elif year == '2016':
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)]["2017"].values[0]
        
        else: 
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)][year].values[0] 
        df['human_capital_index'][i] = value
    except:
        print(country, year)
        df['human_capital_index'][i] = None
        hc_score_errors.append(f"{country} - {year}")

############## master

indicator = "Educational attainment, at least Master's or equivalent, population 25+, total (%) (cumulative)"
masters_errors = []
df['master_degree'] = np.NaN
for i in range(len(df)):
    country = df.countries_corrected[i]
    year = df.start[i][:4]
    try:
        if country == "Unknown" or country == "Worldwide":
            value = None
        elif year == '2022':
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)]["2021"].values[0]

        elif year == '2016':
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)]["2017"].values[0]
        
        else: 
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)][year].values[0] 
        df['master_degree'][i] = value
    except:
        print(country, year)
        df['master_degree'][i] = None
        masters_errors.append(f"{country} - {year}")

indicator = "Educational attainment, at least Bachelor's or equivalent, population 25+, total (%) (cumulative)"
bachelor_errors = []
df['bachelor_degree'] = np.NaN
for i in range(len(df)):
    country = df.countries_corrected[i]
    year = df.start[i][:4]
    try:
        if country == "Unknown" or country == "Worldwide":
            value = None
        elif year == '2022':
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)]["2021"].values[0]

        elif year == '2016':
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)]["2017"].values[0]
        
        else: 
            value = hc_df[(hc_df['Country Name'] == country) & (hc_df['Series Name'] == indicator)][year].values[0] 
        df['bachelor_degree'][i] = value
    except:
        print(country, year)
        df['bachelor_degree'][i] = None
        bachelor_errors.append(f"{country} - {year}")

################ POLITICAL
pol_df['Series Name'].drop_duplicates(keep='first').values.tolist()
indicators = pol_df['Series Name'].drop_duplicates(keep='first').values.tolist()
columns = ['corruption_control', "government_effectiveness", "political_stability", "regulatory_quality", "rule_of_law"]

for indicator, column in zip(indicators, columns):
    df[column] = np.NaN
    for i in range(len(df)):
        country = df.countries_corrected[i]
        year = df.start[i][:4]
        try:
            if country == "Unknown" or country == "Worldwide":
                value = None
            elif year == '2022':
                value = pol_df[(pol_df['Country Name'] == country) & (pol_df['Series Name'] == indicator)]["2021"].values[0]

            elif year == '2016':
                value = pol_df[(pol_df['Country Name'] == country) & (pol_df['Series Name'] == indicator)]["2017"].values[0]
            
            else: 
                value = pol_df[(pol_df['Country Name'] == country) & (pol_df['Series Name'] == indicator)][year].values[0] 
            df[column][i] = value
        except:
            print(country, year)
            df[column][i] = None

    print(f"{indicator} is done!")
            

#########  business indicators

bi_df['Series Name'].drop_duplicates(keep='first').values.tolist()

indicators = bi_df['Series Name'].drop_duplicates(keep='first').values.tolist()
columns = ['ease_of_doing_business', "ease_of_getting_credit", "business_start", "registration_time", "profit_tax_rate"]
business_errors = []
for indicator, column in zip(indicators, columns):
    df[column] = np.NaN
    for i in range(len(df)):
        country = df.countries_corrected[i]
        year = df.start[i][:4]
        try:
            if country == "Unknown" or country == "Worldwide":
                value = None
            elif year == '2022':
                value = bi_df[(bi_df['Country Name'] == country) & (bi_df['Series Name'] == indicator)]["2021"].values[0]

            elif year == '2016':
                value = bi_df[(bi_df['Country Name'] == country) & (bi_df['Series Name'] == indicator)]["2017"].values[0]
            
            else: 
                value = bi_df[(bi_df['Country Name'] == country) & (bi_df['Series Name'] == indicator)][year].values[0] 
            df[column][i] = value
        except:
            
            df[column][i] = None
            business_errors.append(f"{country} - {year}")


    print(f"{indicator} is done!")

business_errors = list(set(business_errors))


df.replace('...', np.NaN, inplace = True)
df.replace('..', np.NaN, inplace = True)
df.replace('', np.NaN, inplace = True)
df.replace(' ', np.NaN, inplace = True)

df.to_csv('data processing/macro_indicators.csv', index = None)


df.info()


media_total = [eval(val) for val in df.media.dropna().values.tolist()]
media_total = [x for list in media_total for x in list]

twitter = 0
facebook = 0
medium = 0
telegram = 0
youtube = 0


len(media_total)
for media in media_total[:100]:
    print(media)



for media in media_total:
    try:
        if "twitter" in media:
            twitter += 1
        elif "facebook" in media:
            facebook += 1
        elif "medium" in media:
            medium += 1
        elif "t.me" in media:
            telegram += 1
        elif "youtube" in media:
            youtube += 1

        else:
            continue
    except:
        print(media)