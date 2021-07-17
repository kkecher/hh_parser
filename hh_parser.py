#!/usr/bin/env python3

"""
Get HH vacancies with regard to filters.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""
#TBD send to Telegram if error

import requests
import json
import datetime
import tests
import sqlite3

HEADERS  = {'user-agent': 'kkecher (kkecher@gmail.com)'}
#Get areas: Москва, Московская область
FILTERS = {'area': [1, 2019], 'date_from': '2021-07-17'}
DATABASE = 'vacancies.db'

def get_areas():
    """
    Get json with geo (countries, regions, cities) and their ids.
    """

    url = 'https://api.hh.ru/areas'
    response = requests.get(url, headers=HEADERS)
    areas = response.json()
    tests.test_get_areas(response, areas)
    return (areas)

def write_areas_to_file(areas):
    """
    Write areas json to file for debugging.
    """

    # Get current time to test writing areas to file success
    current_time = datetime.datetime.now().replace(microsecond=0).isoformat()

    with open ('areas.txt', 'w') as f:
        f.write(current_time)
        f.write(2*'\n')
        f.write(json.dumps(areas, indent=4, ensure_ascii=False))
    tests.test_write_to_file('areas.txt', current_time)
    return ()

def filter_areas(areas):
    """
    Filter geo JSON with regard to FILTERS['area']
    """
    filtered_areas_ids = []
    filtered_areas_indexes = []

    russia = areas[0]
    russian_regions = russia['areas']
    for russian_region_index, russian_region in enumerate(russian_regions):
        if russian_region['name'] in FILTERS['area']:
            filtered_areas_ids.append(russian_region['id'])
            filtered_areas_indexes.append(russian_region_index)
    tests.test_filter_areas(FILTERS['area'], russian_regions, filtered_areas_indexes, filtered_areas_ids)
    return (filtered_areas_ids)

def get_vacancies():
    """
    Get vacancies with regard to FILTERS.
    """
    #TBD for testing: The connection.total_changes method returns the total number of database rows that have been affected.

    vacancy_file = 'vacancies.txt'
    url = 'https://api.hh.ru/vacancies'

    r = requests.get(url, headers=HEADERS, params=FILTERS)
    vacancies = r.json()
    if tests.test_get_vacancies_0(FILTERS, r, vacancies):
        current_time = datetime.datetime.now().replace(microsecond=0).isoformat()
        with open(vacancy_file,  'w') as f:
            f.write(current_time)
            f.write(2*'\n')
            f.write(json.dumps(r.json(), indent=4, ensure_ascii=False))
        tests.test_write_to_file(vacancy_file, current_time)
    else:
        raise ValueError("ERROR! 'get_vacancies' function hasn't passed tests.")
    return (vacancies)

def create_database(table):
    """
    Create the database 'vacancies.db' with table 'hh_vacancies' and row 'id'.
    - This database may contain tables with vacaincies from other sites in future.
    - Other columns will be created during JSON parse.
    """
    con = sqlite3.connect(DATABASE)
    cur = con.cursor()
    create_query = 'CREATE TABLE ' + str(table) + ' (id INTEGER PRIMARY KEY UNIQUE)'
    cur.execute(create_query)
    cur.close()
    con.close()

def write_vacancies_to_database(vacancies, table):
    """
    Write vacancies to SQLite database
    """
    con = sqlite3.connect(DATABASE,
                          detect_types=sqlite3.PARSE_DECLTYPES |
                          sqlite3.PARSE_COLNAMES)
    cur = con.cursor()
    get_columns_query = 'PRAGMA table_info(' + str(table) + ')'
    database_columns = [dummy[1] for dummy in cur.execute(get_columns_query)]

    for vacancy in vacancies['items']:
        print ()
        if tests.test_get_vacancies_1(HEADERS, FILTERS, vacancy):
            vacancy_data = write_vacancy_fields_to_database(vacancy)
            for vacancy_field, vacancy_value in vacancy_data.items():
                if vacancy_field not in database_columns:
                    add_column_query = 'ALTER TABLE ' + str(table) + ' ADD COLUMN ' + str(vacancy_field) + ' STRING'
                    cur.execute(add_column_query)
                    database_columns.append(vacancy_field)
    cur.close()
    con.close()
    print ('The SQLite connection is closed')

def write_vacancy_fields_to_database(vacancy, field_name=''):
    """
    """
    vacancy_data = {}
    for field in vacancy:
        field_name += str(field)
        print ('start_field = ', field)
        print ('vacancy[field] = ', vacancy[field])
        print ()
        if type(vacancy[field]) is dict:
            print ('Arrived to  recursion')
            print ()
            field_name += '_'
            write_vacancy_fields_to_database(vacancy=field, field_name=field_name)
        elif type(vacancy[field]) is not None:
            insert_query = 'INSERT INTO ' + str(table) + ' (' + str(field_name) +  ') VALUES (' + str(vacancy[field]) + ')'
            print ('insert_query = ', insert_query)
            cur.execute(insert_query)
            con.commit()
        field_name = ''
        print ('finish_field = ', field)
        print ()
    return (vacancy_data, database_columns)

def combine_python_database_types():
    pass

def add_database_column():
    pass

areas = get_areas()
write_areas_to_file(areas)
#vacancies = get_vacancies()

#table = 'hh_vacancies'
#try:
#    create_database(table)
#    write_vacancies_to_database(vacancies, table)
#except sqlite3.Error:
#    write_vacancies_to_database(vacancies, table)
#
#print ('Done!')

#TBD use Etag / Cache-Control / Expires to get only new vacancies https://github.com/hhru/api/blob/master/docs_eng/cache.md
