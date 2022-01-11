#!/usr/bin/env python3

import sys
import configparser
import os
# import numpy as np
import pandas as pd
import pymysql
# pymysql.install_as_MySQLdb()
# import re
import csv
from sqlalchemy import create_engine


# Consolidate subset of IMDB data and insert records into MySQL database.

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'db.conf'))

hostname = config.get('default', 'hostname')
username = config.get('default', 'username')
password = config.get('default', 'password')
database = config.get('default', 'database')
csvpath = config.get('default', 'csvpath')


titles_csv = os.path.join(csvpath, 'title.basics.tsv')
names_csv = os.path.join(csvpath, 'name.basics.tsv')
ratings_csv = os.path.join(csvpath, 'title.ratings.tsv')
crew_csv = os.path.join(csvpath, 'title.crew.tsv')


# Create a dictionary of directors using name.basics.tsv

director_names = {}

with open(names_csv, mode='r') as names:
    reader = csv.reader(names, delimiter='\t')
    director_names = {rows[0]: rows[1] for rows in reader}


def get_directors(x):
    dd = [director_names.get(d, '') for d in x]
    # return no more than five directors per movie
    if len(dd) < 5:
        return ', '.join(dd)
    else:
        return dd[0]


# Load titles, crew and ratings and merge into final dataframe

df1 = pd.read_csv(titles_csv, sep='\t', encoding='utf8')
df2 = pd.read_csv(crew_csv, sep='\t', encoding='utf8')
df3 = pd.read_csv(ratings_csv, sep='\t', encoding='utf8')

# Apply some filtering before merging

filtered_df1 = df1.query('isAdult == 0 & titleType == "movie"')

final = pd.merge(pd.merge(filtered_df1, df2, how='inner', on='tconst'),
                 df3, how='left', on='tconst')


# Get each director name by applying get_directors function on each IMDB id

final['directors'] = final['directors'].str.split(',')

final['directors'] = list(map(get_directors, final['directors']))

# Remove unwanted columns

final.drop('writers', axis=1, inplace=True)
final.drop('endYear', axis=1, inplace=True)
final.drop('isAdult', axis=1, inplace=True)

final.rename(columns={'tconst': 'imdb_id',
                      'titleType': 'title_type',
                      'primaryTitle': 'primary_title',
                      'originalTitle': 'original_title',
                      'startYear': 'release_year',
                      'runtimeMinutes': 'running_time',
                      'averageRating': 'rating',
                      'numVotes': 'votes'
                      }, inplace=True)



final['votes'] = final['votes'].fillna(0).astype(int)

final_cleaned = final.replace(
    to_replace='\\N',
    value=None,
    inplace=False,
    limit=None,
    regex=False,
    method='pad')


print(final)
print(list(final.columns.values))


# Connect to the database and insert the data

db_data = 'mysql+mysqldb://{}:{}@{}:3306/{}?charset=utf8mb4'.format(
    username, password, hostname, database)


engine = create_engine(db_data)

connection = pymysql.connect(
    host=hostname, user=username, password=password, db=database)

cursor = connection.cursor()

final_cleaned.to_sql('Film_Titles', engine, if_exists='append', index=False)
