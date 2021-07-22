#!/usr/bin/env python3.6

"""
Get and filter HH vacancies.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""
#TBD send to Telegram if error

import requests
import json
import tests
import sqlite3
import re

HEADERS  = {'user-agent': 'kkecher (kkecher@gmail.com)'}

# The database may contain vacancies from other sites in the future
# so it has name `vacancies.db`, not `hh.db`
DATABASE = 'vacancies.db'

def get_areas(headers, tests):
    """
    Get json with geo (countries, regions, cities) and their ids.
    We'll write this json to sqlite database to filter user required areas.
    """
    print ('Getting geo areas from hh...')

    url = 'https://api.hh.ru/areas'
    response = requests.get(url, headers=headers)
    areas = response.json()
    tests.test_get_areas(response, areas)
    return (areas)

def write_areas_to_file(areas, tests):
    """
    Write json with areas to txt file.
    This function is for debugging only purpose.
    """
    print('Writing geo areas to file...')

    file_name = 'areas.json'

    with open (file_name, 'w') as f:
        f.write(json.dumps(areas, indent=4, ensure_ascii=False))
    tests.test_write_to_file(file_name)
    return ()

def create_areas_database(database):
    """
    Create areas table in vacancies database.
    """
    print ('Creating geo areas database...')

    sqlite_create_table_query = """CREATE TABLE IF NOT EXISTS areas (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER,
    name TEXT NOT NULL COLLATE NOCASE
    );"""

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.execute(sqlite_create_table_query)
    connection.commit()
    cursor.close()
    tests.test_create_areas_database(database)
    if connection:
        connection.close()
    tests.test_create_areas_database(database)
    return ()

def areas_generator(areas):
    """
    Create areas iterator to flatten multilevel json
    (countries > regions > cities) into one-level database table.
    """
    if isinstance(areas, dict):
        for key, value in areas.items():
            if isinstance(value, (dict, list)):
                yield from areas_generator(value)
            else:
                yield (key, value)
    elif isinstance(areas, list):
        for item in areas:
            yield from areas_generator(item)
    else:
        print ()
        print ('Unhandled data type: ', type(areas))
        raise TypeError

def write_areas_to_database(areas_generator, areas, database):
    """
    Iterate over areas generator and fill areas table.
    """
    print ('Writing geo areas to database...')

    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    area = {}
    area_counter = 0
    sqlite_insert_with_param_query = """INSERT OR REPLACE INTO areas
    ('id', 'parent_id', 'name')
    VALUES (?, ?, ?);"""

    for item in areas_generator(areas):
        key, value = item[0], item[1]

        # Lowercase text to have case-insensitive search.
        # `COLLATE NOCASE` doesn't work for cyrillic.
        try:
            value = value.lower()
        except AttributeError:
            pass

        area[key] = value
        if len(area) < 3:
            continue
        else:
            keys = list(area.keys())
            assert keys == ['id', 'parent_id', 'name'],\
                'Expected keys structure: ["id", "parent_id", "name"]\n\
                Got keys structure: %s' % keys
            values = tuple(area.values())
            cursor.execute(sqlite_insert_with_param_query, values)
            connection.commit()
            area_counter += 1
            area = {}
    cursor.close()
    if connection:
        connection.close()
    tests.test_write_areas_to_database(database, area_counter)
    return ()

def is_valid_area_name(name):
    """
    Valid name must follow these rules:
    - can contain only Russian letters
    - can contain numbers
    - can contain characters from string "-().,́’" and whitespaces
    - 1 <= length <= 100 characters
    """
    regex = re.compile(r'[0-9ёЁа-яА-Я-́’,()\s\.]')

    if 1 <= len(name) <= 100 and\
       regex.sub('', name) == '':
        return (True)
    else:
        return (False)

def select_areas_by_name(database, names):
    """
    Select geo areas by name to get their ids.
    """
    print ("Search geo areas...")
    sqlite_select_query = """SELECT * from areas where name like ?"""

    invalid_names, not_found_names, found_names = [], [], []
    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    for name in names:
        if is_valid_area_name(name):
            query_name = '%' + name.lower() + '%'
            cursor.execute(sqlite_select_query, (query_name,))
            found = cursor.fetchall()
            if not found:
                not_found_names.append(name)
            else:
                found_names += found
        else:
            invalid_names.append(name)
    if connection:
        connection.close()
    return (invalid_names, not_found_names, found_names)

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
    #TBD Default get 1 day old vacancies  for Moscow and Moscow Oblast.
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

# areas = get_areas(HEADERS, tests)
# write_areas_to_file(areas, tests)
# create_areas_database(DATABASE)
# write_areas_to_database(areas_generator, areas, DATABASE)
tests.test_is_valid_area_name(is_valid_area_name)
tests.test_select_areas_by_name(select_areas_by_name, DATABASE)

names = [
    'влад',
    'москва',
    'петербург',
    'drop database',
    'благов',
    'дальнег',
    'nyc',
    'жоп',
    'ху',
    'пиз',
]
invalid_names, not_found_names, found_names = \
    select_areas_by_name(DATABASE, names)

print ()
print ('Invalid names: ')
for invalid_name in invalid_names:
    print (invalid_name)
print ()
print ('Not found names: ')
for not_found_name in not_found_names:
    print (not_found_name)
print ()
print ('Found names: ')
for found_name in found_names:
    print (found_name)

#print (
#    'Where do you want to search vacancies?\n\
#    Write cities, regions or countries on Russian separated by commas \
#    and press `ENTER`.'
#)
#user_areas_id = []
#user_area = ''
#
#while user_area.lower() != 'stop' or user_area.lower() != 'стоп':
#    user_area = input()
#    # TBD search user_area at db,
#    # return area name if success, return error otherwise
#    # user_area_id = db search
#    user_areas_id.append(user_area_id)


#vacancies = get_vacancies()

#table = 'hh_vacancies'
#try:
#    create_database(table)
#    write_vacancies_to_database(vacancies, table)
#except sqlite3.Error:
#    write_vacancies_to_database(vacancies, table)
#
print ()
print ('Done!')

#TBD use Etag / Cache-Control / Expires to get only new vacancies https://github.com/hhru/api/blob/master/docs_eng/cache.md
