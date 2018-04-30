import pandas as pd
import sqlite3
import numpy as np

"""
This script cleans and loads both your csv files and loads them into an SQLite
database in one run.
"""
#Load
s1 = pd.read_csv('./data/study_101.csv')
s2 = pd.read_csv('./data/study_102.csv')

#Clean
s1['date'] = s1['date'].apply(lambda x: pd.to_datetime(x, format='%Y-%d-%M').strftime('%d/%M/%Y'))
s1['study_id'] = 101
s2['date'] = s2['date'].apply(lambda x: pd.to_datetime(x, format='%M/%d/%Y').strftime('%d/%M/%Y'))
s2['study_id'] = 102
combine = s1.append(s2)
for i in range(1,7):
    j = str(i)
    combine['Q3_'+j] = combine['Q3'].apply(lambda x: j in x).astype('int')
combine.drop(['Q3'], axis=1, inplace=True)
combine.reset_index(drop=True, inplace=True)
combine.rename(columns={'Number of children':'n_children'}, inplace=True)

#This is where customization takes place after cleaning

#Respondents
respn1 = combine[['respondent_id','study_id','Sex','date','n_children']]
respn1.rename(columns={'respondent_id':'id', 'Sex':'sex'}, inplace=True)

#Responses
new_df = combine[['respondent_id','study_id','Q1','Q2','Q3_1','Q3_2','Q3_3','Q3_4','Q3_5','Q3_6','Q4','Q5']]
newer_df = pd.melt(new_df, id_vars=['respondent_id','study_id'],value_vars=['Q1','Q2','Q3_1','Q3_2','Q3_3','Q3_4','Q3_5','Q3_6','Q4','Q5'])
newer_df = newer_df.sort_values(['study_id','respondent_id'])
newer_df = newer_df.reset_index(drop=True)
newer_df = newer_df.reset_index() #Again for the column
newer_df['index'] = newer_df['index']+1
newer_df = newer_df.rename(columns={'index':'id'})

#Connect to SQLite
sqlite_db = './dbase.sqlite'
conn = sqlite3.connect(sqlite_db)

#Codebook loading will cause an error, so I will be skipping that
#Load questions here
question = pd.read_csv('./sample database schema/questions_table.csv')
question.to_sql('question', con=conn, if_exists = 'replace', index = False)

#Load the rest in
respn1.to_sql('respondent', con=conn, if_exists = 'replace', index=False, dtype={'id':'INT','study_id':'INT','sex':'INT','n_children':'INT','date':'TEXT'})
newer_df.to_sql('response', con=conn, if_exists = 'replace', index=False, dtype={'id':'INT','study_id':'INT','respondent_id':'INT','question_id':'TEXT','response':'INT'})
