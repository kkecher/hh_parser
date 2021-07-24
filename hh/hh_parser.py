#!/usr/bin/env python3.6

"""
Get and filter hh vacancies.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

import requests
import json
import tests
import sqlite3
import re

HEADERS  = {"user-agent": "kkecher (kkecher@gmail.com)"}

# The database may contain vacancies from other sites in the future
# so it has name `vacancies.db`, not `hh.db`
DATABASE = "vacancies.db"

def get_areas(headers, tests):
    """
    Get json with geo (countries, regions, cities) and their ids.
    We'll write this json to sqlite database to search by areas.
    """
    print ('Getting geo areas from hh...')

    url = 'https://api.hh.ru/areas'
    response = requests.get(url, headers=headers)
    areas = response.json()
    tests.test_get_areas(response, areas)
    return (areas)

def write_to_file(file_name, json_data, tests):
    """
    Write json to file.
    This function is for debugging only purpose.
    """
    print(f'Writing data to file `{file_name}`...')

    with open (file_name, 'w') as f:
        f.write(json.dumps(json_data, indent=4, ensure_ascii=False))
    tests.test_write_to_file(file_name)
    return ()

def create_table(database, table, columns):
    """
    Create table at `database`.
    `columns`: list of strings with columns name and attributes
    """
    print (f'Creating table `{table}` at `{database}` database...')

    query = f'CREATE TABLE {table} (
    {column for column in columns},
    );'
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    if connection:
        connection.close()
    tests.test_create_database(database)
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

def write_areas_to_database(db, areas_generator, areas):
    """
    Iterate over areas generator and fill `areas` table.
    """
    print ('Writing geo areas to database...')

    connection = sqlite3.connect(db)
    cursor = connection.cursor()

    area = {}
    area_counter = 0
    sqlite_insert_with_param_query = """INSERT OR REPLACE INTO areas\
    ('id', 'parent_id', 'name') VALUES (?, ?, ?);"""

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
    database_changes_number = connection.total_changes
    if connection:
        connection.close()

    tests.test_write_to_database(database_changes_number, area_counter)
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

    invalid_names, not_found_names, found_names = set(), set(), set()

    # It will be used to clean duplicates
    found_ids = set()

    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    for name in names:
        if is_valid_area_name(name):
            query_name = '%' + name.lower() + '%'
            cursor.execute(sqlite_select_query, (query_name,))
            found = cursor.fetchall()
            if not found:
                not_found_names.add(name)
            else:
                for found_area in found:
                    found_names.add(found_area)
                    found_ids.add(found_area[0])
        else:
            invalid_names.add(name)
    if connection:
        connection.close()
    return (invalid_names, not_found_names, found_names, found_ids)

def clean_area_duplicates(found_names, found_ids):
    """
    If `select_areas_by_name` returns some areas
    which are children of others, this function removes such children areas.

    `found_names` is set of tuples with structure {(`id`, `parent_id`, `name`)}.
    """
    cleaned_names = set()
    cleaned_ids = set()

    for found_name in found_names:
        parent_id = found_name[1]
        if parent_id not in found_ids:
            cleaned_names.add(found_name)
            cleaned_ids.add(found_name[0])
    tests.test_clean_area_duplicates(found_names, cleaned_names)
    return (cleaned_names, cleaned_ids)

def get_vacancies(headers, filters):
    """
    Get vacancies under `filters`.
    """
    print ('Get vacancies...')

    url = 'https://api.hh.ru/vacancies'

    response = requests.get(url, headers=headers, params=filters)
    vacancies = response.json()
    tests.test_get_vacancies(response, vacancies, filters)
    return (vacancies)

def vacancies_generator(vacancies, parent_key = ''):
    """
    Create vacancies iterator to flatten multilevel json
    into one-level database table.
    """
    if isinstance(vacancies, dict):
        for key, value in vacancies.items():
            if isinstance(value, (dict, list)):
                yield from vacancies_generator(value, parent_key+key+'_')
            else:
                yield (parent_key+key, value)
    elif isinstance(vacancies, list):
        for item in vacancies:
            yield from vacancies_generator(item, parent_key)
    else:
        print ()
        print ('Unhandled data type: ', type(vacancies))
        raise TypeError

def write_vacancies_to_database(db, vacancies_generator, vacancies):
    """
    Iterate over vacancies generator and fill `vacancies` table.
    """
    print ()
    print ('Writing vacancies to database...')

    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    table_columns = [column[0] for column in\
               cursor.execute("""SELECT * FROM vacancies""").description]
    print ('table_columns = ', table_columns)

    vacancy = {}
    vacancy_counter = 0

    for item in vacancies_generator(vacancies):
        key, value = item[0], item[1]

        # Lowercase text to have case-insensitive search.
        # `COLLATE NOCASE` doesn't work for cyrillic.
        try:
            value = value.lower()
        except AttributeError:
            pass

        vacancy[key] = value
        if key == 'id' and len(vacancy) > 1:
            print('vacancy = ', vacancy)
            keys = vacancy.keys()
            values = vacancy.values()
            for key in keys:
                if key not in table_columns:
                    cursor.execute(
                        'ALTER TABLE vacancies ADD COLUMN %s TEXT' % key
                    )
                    table_columns.append(key)
            # cursor.execute(
            #     '''INSERT OR REPLACE INTO vacancies (%s) VALUES (?)''' %\
            #     (keys), (values,)
            # )
            connection.commit()
            vacancy_counter += 1
            vacancy = {}
        else:
            continue

    cursor.close()
    database_changes_number = connection.total_changes
    if connection:
        connection.close()

    # tests.test_write_to_database(database_changes_number, vacancy_counter)
    return ()

def write_vacancies_to_database_old(database, table, vacancies):
    """
    Write vacancies json to `table`.
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

areas = get_areas(HEADERS, tests)
write_to_file('areas.json', areas, tests)

# Create table `areas`


# write_areas_to_database(DATABASE, areas_generator, areas)
tests.test_is_valid_area_name(is_valid_area_name)
tests.test_select_areas_by_name(select_areas_by_name, DATABASE)

names = [
    'Москва',
    'московская область'
]
invalid_names, not_found_names, found_names, found_ids = \
    select_areas_by_name(DATABASE, names)

cleaned_names, cleaned_ids = clean_area_duplicates(found_names, found_ids)

print ('cleaned_names = ', cleaned_names)
print ('cleaned_ids = ', cleaned_ids)

# create_table(
#     DATABASE,
#     """CREATE TABLE IF NOT EXISTS vacancies (
#     id INTEGER PRIMARY KEY
#     );"""
# )
vacancy_filters = {
    'area': list(cleaned_ids),
    'period': '1'
}

vacancies = get_vacancies(HEADERS, vacancy_filters)['items']
#write_to_file('vacancies.json', vacancies, tests)

write_vacancies_to_database(DATABASE, vacancies_generator, vacancies)

# print ()
# print ('Invalid names: ')
# for invalid_name in invalid_names:
#     print (invalid_name)
# print ()
# print ('Not found names: ')
# for not_found_name in not_found_names:
#     print (not_found_name)
# print ()
# print ('Found names: ')
# for found_name in found_names:
#     print (found_name)

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


create_areas_query = '    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE'

def main():
    pass

if __name__ == "__main__":
    main()
