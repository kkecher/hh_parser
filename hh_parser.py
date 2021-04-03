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

HEADERS  = {'user-agent': 'kkecher (kkecher@outlook.com)'}
FILTERS = {'areas': ['Москва', 'Московская область']}

def get_areas():
    """
    Get JSON with geo (countries, regions, cities) and their ids.
    """
    url = 'https://api.hh.ru/areas'
    r = requests.get(url, headers=HEADERS)
    areas = r.json()
    tests.test_get_areas(r, areas)

    #Get current  time to test writing to file
#    write_to_file_time = datetime.datetime.now().replace(microsecond=0).isoformat()
#    with open ('areas.txt', 'w') as f:
#        f.write(write_to_file_time)
#        f.write(2*'\n')
#        f.write(json.dumps(areas, indent=4, ensure_ascii=False))
#        tests.test_write_to_file('areas.txt', write_to_file_time)        
    return (areas)

def filter_areas(areas):
    """
    Filter geo JSON with regard to FILTERS['areas']
    """
    filtered_areas_ids = []
    filtered_areas_indexes = []
    
    russia = areas[0]
    russian_regions = russia['areas']
    for russian_region_index, russian_region in enumerate(russian_regions):
        if russian_region['name'] in FILTERS['areas']:
            filtered_areas_ids.append(russian_region['id'])
            filtered_areas_indexes.append(russian_region_index)
    tests.test_filter_areas(FILTERS['areas'], russian_regions, filtered_areas_indexes, filtered_areas_ids)
    return (filtered_areas_ids)

def get_vacancies(filtered_areas_ids):
    """
    Get vacancies with regard to FILTERS['areas'].
    """
    area_filter = {'area': filtered_areas_ids}
    vacancy_file = 'vacancies.txt'
    url = 'https://api.hh.ru/vacancies'
    
    r = requests.get(url, headers=HEADERS, params=area_filter)
    vacancies = r.json()
    if tests.test_get_vacancies(filtered_areas_ids, r, vacancies, HEADERS):
        write_to_file_time = datetime.datetime.now().replace(microsecond=0).isoformat()
        with open(vacancy_file,  'w') as f:
            f.write(write_to_file_time)
            f.write(2*'\n')
            f.write(json.dumps(r.json(), indent=4, ensure_ascii=False))
        tests.test_write_to_file(vacancy_file, write_to_file_time)
    else:
        raise ValueError("ERROR! get_vacancies function hasn't passed tests.")
    return ()

#areas = get_areas()
#filtered_areas_ids = filter_areas(areas)
#get_vacancies(filtered_areas_ids)

try:
    sqlite_connection = sqlite3.connect('hh_vacancies.db')
    cursor = sqlite_connection.cursor()
    sqlite_select_query = 'select sqlite_version();'
    cursor.executre(sqlite_select_query)
    record = cursor.fetchall()
    print ('SQLite Database Version is: ', record)
    cursor.close()
except sqlite3.Error as error:
    print ('Error while connectiong to SQLite', error)
finally:
    if sqlite_connection:
        sqlite_connection.close()
        print ('The SQLite connection is closed')
        
print ('Done!')

#TBD use Etag / Cache-Control / Expires to get only new vacancies https://github.com/hhru/api/blob/master/docs_eng/cache.md
