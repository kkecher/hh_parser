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

from get_areas import AREAS_TABLE

from common_functions import (
    HEADERS,
    DATABASE,
    write_to_file,
    create_table,
    get_table_columns_names,
    get_table_columns_names,
    create_table_columns,
    insert_into_table,
    rename_json_to_database_key
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

VACANCIES_TABLE = "hh_vacancies"
VAСANCIES_FILE = "./data/vacancies.json"
JSON_DATABASE_KEY_MATCHES = {}

def get_vacancies(headers, filters):
    """
    Get vacancies under `filters`.
    """
    in_tests.test_get_vacancies(headers, filters)
    print ("Getting vacancies...")

    url = "https://api.hh.ru/vacancies"
    response = requests.get(url, headers=headers, params=filters)
    vacancies = response.json()
    out_tests.test_get_vacancies(response, vacancies, filters)
    return (vacancies)

def vacancies_generator(vacancies, parent_key = ""):
    """
    Create vacancies iterator to flatten multilevel json
    into one-level database table.
    """
    in_tests.test_json_data_type(vacancies)

    if isinstance(vacancies, dict):
        for key, value in vacancies.items():
            if isinstance(value, (dict, list)):
                yield from vacancies_generator(value, parent_key+key+"_")
            else:
                yield (parent_key+key, value)
    elif isinstance(vacancies, list):
        for item in vacancies:
            yield from vacancies_generator(item, parent_key)
    else:
        print ()
        print ("Unhandled data type: ", type(vacancies))
        raise TypeError

def write_vacancies_area_to_database():
    return ()

def write_vacancies_address_to_database():
    # can be null #dd
    # fill it as is except metro stations #dd
    return ()

def write_vacancies_metro_stations_to_database():
    return ()

def write_vacancies_employer_to_database():
    return ()

def write_vacancies_to_database(database, table, vacancies_generator):
    """
    Iterate over vacancies generator and fill `vacancies` table.
    """
    in_tests.test_write_to_database_from_generator(
        database, table, vacancies_generator)
    print (f"Writing vacancies to `{database} > {table}`...")

    columns_names = get_table_columns_names(database, table)
    vacancy = {}
    vacancies_counter = 0
    database_changes_count = 0

    # These keys are stored in separate tables of the database.
    # They are processed by separate functions.
    special_keys = {
        "area": write_vacancies_area_to_database,
        "address": write_vacancies_address_to_database,
        "metro_stations": write_vacancies_metro_stations_to_database,
        "employer": write_vacancies_employer_to_database
        }

    for item in vacancies_generator:
        key, value = item[0], item[1]
        key = rename_json_to_database_key(key)

        # Lowercase text to have case-insensitive search.
        # `COLLATE NOCASE` doesn't work for cyrillic.
        try:
            value = value.lower()
        except AttributeError:
            pass

        if key in special_keys:
            key_function = special_keys[key]
            key_function(database, key)
            continue

        if key not in columns_names:
            column = [(f"{key} TEXT")]
            create_table_columns(database, table, column)
            columns_names.append(key)

        if key != "id" or vacancy == {}:
            vacancy[key] = value
        else:
            database_changes_count += insert_into_table(database, table, vacancy)
            vacancy_counter += 1
            vacancy[key] = value
    database_changes_count += insert_into_table(database, table, vacancy)
    vacancy_counter += 1
    out_tests.test_write_to_database(database_changes_count, vacancy_counter)
    return ()

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

    # out_tests.test_write_areas_to_database(
    #     database_changes_count, vacancy_counter)
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

def main():
    filters = {"area": [2019]}
    vacancies = get_vacancies(HEADERS, filters)
    write_to_file(VAСANCIES_FILE, vacancies)

    create_table(DATABASE, "streets",\
    ["area_id INTEGER NOT NULL", "city_name TEXT",\
     "street_name TEXT",\
     "PRIMARY KEY (area_id, city_name, street_name)",\
     f"FOREIGN KEY (area_id) REFERENCES {AREAS_TABLE} (id)\
     ON UPDATE CASCADE ON DELETE RESTRICT"])

    create_table(DATABASE, "metro_stations",\
    ["station_id TEXT NOT NULL PRIMARY KEY",\
     "station_name TEXT NOT NULL", "line_name TEXT NOT NULL",\
     "station_lat TEXT", "station_lng TEXT"])

    create_table(DATABASE, "employers",\
    ["id INTEGER NOT NULL PRIMARY KEY", "name TEXT NOT NULL",\
     "url TEXT NOT NULL", "alternate_url TEXT NOT NULL",\
     "logo_url_original TEXT", "logo_url_240 TEXT",\
     "logo_url_90 TEXT",\
     "vacancies_url TEXT NOT NULL", "is_trusted INTEGER"])

    create_table(DATABASE, "vacancies_metro_stations",\
    ["vacancy_id INTEGER NOT NULL", "metro_station_id TEXT NOT NULL",\
    "PRIMARY KEY (vacancy_id, metro_station_id)",\
    f"FOREIGN KEY (vacancy_id) REFERENCES {VACANCIES_TABLE} (id)\
    ON UPDATE CASCADE ON DELETE CASCADE",\
    f"FOREIGN KEY (metro_station_id) REFERENCES metro_stations (station_id)\
    ON UPDATE CASCADE ON DELETE CASCADE"])

    create_table(DATABASE, VACANCIES_TABLE,\
    ["id INTEGER NOT NULL PRIMARY KEY", "area_id INTEGER",\
     "address_city  TEXT", "address_street TEXT", "metro_station_id TEXT",\
     "employer_id TEXT",\
     f"FOREIGN KEY (area_id) REFERENCES {AREAS_TABLE} (id)\
    ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (area_id, address_city, address_street)\
     REFERENCES streets (area_id, city_name, street_name)\
     ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (employer_id) REFERENCES employers (id)\
    ON UPDATE CASCADE ON DELETE RESTRICT"])

    # write_vacancies_to_database(
    # DATABASE, VACANCIES_TABLE, vacancies_generator(vacancies))

    for key, value in vacancies_generator(vacancies["items"], parent_key = ""): #dd
        print () #dd
        print (f"{key} = {value}") #dd
        input ("enter") #dd

    print ()
    print ('Done!')

if __name__ == "__main__":
    main()
