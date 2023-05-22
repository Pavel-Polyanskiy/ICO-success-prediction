import pandas as pd


tm_ner_desc = pd.read_csv("feature extraction/whitepaper analysis/data/processed_data/TM_NER_DESC.csv")
ri = pd.read_csv("feature extraction/whitepaper analysis/data/processed_data/readability_index.csv")
cat_vars = pd.read_excel("feature extraction/whitepaper analysis/data/processed_data/categorical_processed.xlsx")
control_vars = pd.read_excel("feature extraction/whitepaper analysis/data/processed_data/control_variables.xlsx")
macro_vars = pd.read_excel("feature extraction/whitepaper analysis/data/processed_data/macro_indicators_processed.xlsx")
num_vars = pd.read_excel("feature extraction/whitepaper analysis/data/processed_data/numerical_processed.xlsx")

tm_ner_desc.merge(ri, on = 'name', how = 'left')

dfs = [ri, cat_vars, control_vars, macro_vars, num_vars]

total = tm_ner_desc.copy()
for df in dfs:
    total = total.merge(df, on = 'name', how = 'left')

to_drop = ['start_y', 'end_y']
total.drop(to_drop, axis = 1, inplace = True)
total.rename(columns = {'start_x': 'start', 'end_x': 'end'}, inplace = True)

total.shape
total.soft_cap.replace('ERROR', None, inplace =  True)
total.hard_cap.replace('ERROR', None, inplace =  True)

total.soft_cap = total.soft_cap.astype(float)
total.hard_cap = total.hard_cap.astype(float)

total[['name', 'ticker']].to_csv('feature extraction/social media data extraction/ticker2name.csv', index = None)
total.shape

###duration 

duration = (pd.to_datetime(total['end']) - pd.to_datetime(total['start'])).dt.days

total['duration'] = duration


### team size

total['team_size'] = total.team.apply(lambda team: len(eval(team)))

sum(total['team_size'] > 0)
#### verified %

def find_verified_team(team):
    team = eval(team)
    team_size = len(team)
    verified = len([name for name in team.keys() if str(team[name]).startswith("http") or str(team[name]).startswith("www")])
    verified_percentage = verified / team_size

    return verified_percentage


total['verified_team_share'] = total.team.apply(lambda team: find_verified_team(team))

#### target

total[['soft_cap', 'raised']].info()

soft_cap_copy = total["soft_cap"]

for i in range(len(total)):
    if pd.isnull(total["soft_cap"][i]):
        total["soft_cap"][i] = total["raised"][i]

    else:
        continue

total['success_binary'] = total.apply(lambda row: 1 if row['raised'] >= row['soft_cap'] else 0, axis = 1)

total['success_percentage'] = total['raised'] / total['soft_cap']

###### non-featured columns

total.columns

to_drop_2 = ['text','team', 'start', 'end', 'media']

total.drop(to_drop_2, inplace = True, axis = 1)

total.drop(['ticker', 'countries_corrected'] , inplace = True, axis = 1)

total.shape

total.to_csv("feature extraction/whitepaper analysis/data/processed_data/all_features.csv", index = None)


total = pd.read_csv("feature extraction/whitepaper analysis/data/processed_data/all_features.csv")


### adding layouts
layout = pd.read_csv("feature extraction/whitepaper analysis/data/processed_data/layout.csv")

total = total.merge(layout, on = 'name', how = 'left')

total.to_csv("feature extraction/whitepaper analysis/data/processed_data/all_features.csv", index = None)

#### adding sentiment

sent = pd.read_csv('feature extraction/social media data extraction/sentiment_scores.csv')
sent = sent[['name', 'sentiment']]

total = total.merge(sent, on = 'name', how = 'left')




