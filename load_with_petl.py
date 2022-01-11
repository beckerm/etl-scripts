#!/usr/bin/env python3

import configparser
import os
import petl as etl
import pymysql
import argparse
import sys
# import re
# pymysql.install_as_MySQLdb()


config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'db.conf'))

hostname = config.get('default', 'hostname')
username = config.get('default', 'username')
password = config.get('default', 'password')
database = config.get('default', 'database')


parser = argparse.ArgumentParser(
    description='Import IMDB data.')
parser.add_argument('-f', '--file', help='File name.', required=True)

args = parser.parse_args()

csv_file = args.file


def load_csv_data(f):

    csv_table = etl.fromcsv(f, delimiter='\t')

    # table2 = etl.addfield(csv_table, 'Imported', lambda rec: 'Y')
    # pattern = re.compile(r'taxi', re.IGNORECASE)
    # table4 = etl.search(table3, 'originalTitle', pattern)

    # print(csv_table.lookall())

    # Grab only movie types.
    csv_table_clean_movies = etl.select(
        csv_table, lambda x: x.titleType == 'movie' and x.isAdult == '0')

    # Remove uwanted columns.
    csv_table_clean_movies_load = etl.cutout(
        csv_table_clean_movies, 'originalTitle', 'endYear', 'isAdult')

    # d = etl.dicts(table3)
    # print(list(d))

    # To list
    # d = etl.data(table3)
    # print(list(d))

    # for i in list(d):
    #     print(i)

    # Map column headers to table column names.
    table_to_insert = etl.rename(csv_table_clean_movies_load, {
        'titleType': 'title_type',
        'primaryTitle': 'movie_title',
        'startYear': 'release_year',
        'runtimeMinutes': 'running_time',
        'genres': 'genre',
        'tconst': 'imdb_id',
    })

    def connect_mysql():
        try:
            return pymysql.connect(host=hostname, user=username, passwd=password, db=database, charset='utf8')
        except Exception as e:
            print('Error connecting to database: {}'.format(e))
            sys.exit()

    db = connect_mysql()
    movie_cursor = db.cursor()

    if etl.nrows(table_to_insert) > 0:

        movie_cursor.execute('SET SQL_MODE=ANSI_QUOTES')
        etl.todb(table_to_insert, movie_cursor, 'Films')

        return etl.nrows(table_to_insert)

    else:
        print('No data. Nothing loaded.')
        sys.exit()


if os.path.exists(csv_file):

    total_records = load_csv_data(csv_file)
    print('{} records loaded.'.format(total_records))

else:
    print('File {} not found.'.format(csv_file))
    sys.exit()
