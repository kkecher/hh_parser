#!/usr/bin/env python3

"""
Get and filter hh vacancies.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

import json
import sqlite3

import requests

import tests.input_tests as in_tests
import tests.output_tests as out_tests

HEADERS  = {"user-agent": "kkecher (kkecher@gmail.com)"}

# The database may contain vacancies from other sites in future
# so it has name `vacancies.db`, not `hh.db`
DATABASE = "./data/vacancies.db"
VACANCIES_TABLE = "hh"
AREAS_TABLE = "areas"
AREAS_FILE = "./data/areas.json"


def get_areas(headers):
    """
    Get json with geo (countries, regions, cities) and their ids.
    We'll write this json to sqlite database to search by areas.
    """
    in_tests.test_get_areas_headers(headers)
    print ("Getting geo areas from hh...")

    url = "https://api.hh.ru/areas"
    response = requests.get(url, headers=headers)
    areas = response.json()
    out_tests.test_get_areas(response, areas)
    return (areas)

def write_to_file(file_name, json_data):
    """
    Write json to file.
    This function is for debugging only purpose.
    """
    in_tests.test_write_to_file(file_name, json_data)
    print(f"Writing json to `{file_name}`...")

    with open (file_name, "w") as f:
        f.write(json.dumps(json_data, indent=4, ensure_ascii=False))
    out_tests.test_write_to_file(file_name)
    return ()

def create_table(database, table, columns):
    """
    Create table at database.
    `columns`: list of strings with columns params
    """
    in_tests.test_create_table_columns(database, table, columns)
    print (f"Creating table `{table}` at `{database}` database...")

    query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    columns = ("id",)
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
    out_tests.test_create_table_columns(
        database, table, get_table_columns_names, columns)
    return ()

def areas_generator(areas):
    """
    Create areas iterator to flatten multilevel json
    (countries > regions > cities) into one-level database table.
    """
    in_tests.test_json_data_type(areas)

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
        print ("Unhandled data type: ", type(areas))
        raise TypeError

def get_table_columns_names(database, table):
    """
    Get table column names.
    """
    in_tests.test_get_table_columns_names(database, table)
    print (f"Getting `{database} > {table}` column names...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = "PRAGMA table_info(" + str(table) + ")"
    columns = list(cursor.execute(query))
    columns_names = [column[1] for column in columns]

    cursor.close()
    connection.close()
    out_tests.test_get_table_columns_names(columns_names)
    return (columns_names)

def create_table_columns(database, table, columns):
    """
    Create columns in table at database.
    `columns`: list of strings with columns params
    """
    in_tests.test_create_table_columns(database, table, columns)

    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    for column in columns:
        query = f"ALTER TABLE {table} ADD COLUMN {column}"
        cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()

    out_tests.test_create_table_columns(
        database, table, get_table_columns_names, columns)
    return ()

def insert_into_table(database, table, data):
    """
    Insert or replace data into table at database.
    `data` = dict of query {key: value}
    """
    in_tests.test_insert_into_table(database, table, data)
    print (f"Insert or update data in `{database} > {table}`...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query_columns = ", ".join(data.keys())
    query_values = f"{'?, ' * len(data)}"[:-2]
    query =\
f"INSERT OR REPLACE INTO {table} ({query_columns}) VALUES ({query_values});"
    cursor.execute(query, list(data.values()))
    connection.commit()
    database_changes_count = connection.total_changes
    cursor.close()
    connection.close()
    out_tests.test_insert_into_table(database_changes_count)
    return (database_changes_count)

def write_areas_to_database(database, table, areas_generator):
    """
    Iterate over areas generator and fill database table.
    """
    in_tests.test_write_areas_to_database(database, table, areas_generator)
    print (f"Writing geo areas to `{database} > {table}`...")

    columns_names = get_table_columns_names(database, table)
    area = {}
    area_counter = 0
    database_changes_count = 0

    for item in areas_generator:
        key, value = item[0], item[1]

        # Lowercase text to have case-insensitive search.
        # `COLLATE NOCASE` doesn't work for cyrillic.
        try:
            value = value.lower()
        except AttributeError:
            pass

        if key not in columns_names:
            column = [(f"{key} TEXT")]
            create_table_columns(database, table, column)
            columns_names.append(key)

        if key != "id" or area == {}:
            area[key] = value
        else:
            database_changes_count += insert_into_table(database, table, area)
            area_counter += 1
            area[key] = value
    database_changes_count += insert_into_table(database, table, area)
    area_counter += 1
    out_tests.test_write_areas_to_database(database_changes_count, area_counter)
    return ()

def select_areas_by_name(database, table, names):
    """
    Select geo areas by name to get their ids.
    `names` = list of names for search.
    """
    in_tests.test_select_areas_by_name(database, table, names)
    print ("Search geo areas...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"SELECT * FROM {table} WHERE name LIKE ?"
    not_found_names, found_names, found_ids = set(), set(), set()

    for name in names:
        query_name = f"%{name.lower()}%"
        cursor.execute(query, (query_name,))
        found = cursor.fetchall()
        if not found:
            not_found_names.add(name)
        else:
            for found_area in found:
                found_names.add(found_area)
                found_ids.add(found_area[0])
    cursor.close()
    connection.close()
    out_tests.test_select_areas_by_name(not_found_names, found_names, found_ids)
    return (not_found_names, found_names, found_ids)

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
    out_tests.test_clean_area_duplicates(found_names, cleaned_names)
    return (cleaned_names, cleaned_ids)

def get_vacancies(headers, filters):
    """
    Get vacancies under `filters`.
    """
    print ('Get vacancies...')

    url = 'https://api.hh.ru/vacancies'

    response = requests.get(url, headers=headers, params=filters)
    vacancies = response.json()
    out_tests.test_get_vacancies(response, vacancies, filters)
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

def write_vacancies_to_database(database, vacancies_generator, vacancies):
    """
    Iterate over vacancies generator and fill `vacancies` table.
    """
    print ()
    print ('Writing vacancies to database...')

    connection = sqlite3.connect(database)
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
    database_changes_count = connection.total_changes
    if connection:
        connection.close()

    # out_tests.test_write_areas_to_database(database_changes_count, vacancy_counter)
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
        if out_tests.test_get_vacancies_1(HEADERS, FILTERS, vacancy):
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



# Create table `areas`


# write_areas_to_database(DATABASE, areas_generator, areas)
# out_tests.test_select_areas_by_name(select_areas_by_name)
# out_tests.test_select_areas_by_name(select_areas_by_name, DATABASE)

# names = [
#     'Москва',
#     'московская область'
# ]
# invalid_names, not_found_names, found_names, found_ids = \
#    select_areas_by_name(DATABASE, names)

# cleaned_names, cleaned_ids = clean_area_duplicates(found_names, found_ids)
#
# print ('cleaned_names = ', cleaned_names)
# print ('cleaned_ids = ', cleaned_ids)

# create_table(
#     DATABASE,
#     """CREATE TABLE IF NOT EXISTS vacancies (
#     id INTEGER PRIMARY KEY
#     );"""
# )
# vacancy_filters = {
#     'area': list(cleaned_ids),
#     'period': '1'
# }
#
# vacancies = get_vacancies(HEADERS, vacancy_filters)['items']
#write_to_file('vacancies.json', vacancies, out_tests)

# write_vacancies_to_database(DATABASE, vacancies_generator, vacancies)

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
#    # TBD search user_area at database,
#    # return area name if success, return error otherwise
#    # user_area_id = database search
#    user_areas_id.append(user_area_id)


#vacancies = get_vacancies()

#table = 'hh_vacancies'
#try:
#    create_database(table)
#    write_vacancies_to_database(vacancies, table)
#except sqlite3.Error:
#    write_vacancies_to_database(vacancies, table)
#


#TBD use Etag / Cache-Control / Expires to get only new vacancies https://github.com/hhru/api/blob/master/docs_eng/cache.md


# create_areas_query = '    id INTEGER PRIMARY KEY,
#     name TEXT NOT NULL COLLATE NOCASE'

def main():
    areas = get_areas(HEADERS)
    # write_to_file(AREAS_FILE, areas)
    # create_table(
    #     DATABASE,
    #     AREAS_TABLE,
    #     ["id INTEGER NOT NULL PRIMARY KEY"]
    # )
    # write_areas_to_database(DATABASE, AREAS_TABLE, areas_generator(areas))
    names = [
        "моск",
        "владиво"
    ]
    not_found_names, found_names, found_ids =\
        select_areas_by_name(DATABASE, AREAS_TABLE, names)
    print (f"not_found_names = {not_found_names}") #dd
    print (f"found_names = {found_names}") #dd
    print (f"found_ids = {found_ids}") #dd
    print ()
    print ('Done!')

if __name__ == "__main__":
    main()
