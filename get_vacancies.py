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

from general import (
    HEADERS,
    DATABASE,
    write_to_file,
    create_table,
    get_table_columns_names,
    get_table_columns_names,
    create_table_columns,
    insert_into_table
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

VACANCIES_TABLE = "hh"
VAСANCIES_FILE = "./data/vacancies.json"

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

def write_vacancies_to_database(database, table, vacancies_generator):
    """
    Iterate over vacancies generator and fill `vacancies` table.
    """
    in_tests.test_write_to_database_from_generator(database, table, vacancies_generator)
    print (f"Writing vacancies to `{database} > {table}`...")

    columns_names = get_table_columns_names(database, table)
    vacancy = {}
    vacancies_counter = 0
    database_changes_count = 0

    for item in vacancies_generator:
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
    out_tests.test_write_to_database(database_changes_count, area_counter)
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
    filters = {"area": [22]}
    vacancies = get_vacancies(HEADERS, filters)
    write_to_file(VAСANCIES_FILE, vacancies)
    # create_table(
    #     DATABASE,
    #     VACANCIES_TABLE,
    #     ["id INTEGER NOT NULL PRIMARY KEY"]
    # )
    write_vacancies_to_database(DATABASE, VACANCIES_TABLE, vacancies_generator(vacancies))
    for key, value in vacancies_generator(vacancies["items"], parent_key = ""):
        print (f"{key} = {value}") #dd
        input ("enter") #dd
        
    print ()
    print ('Done!')

if __name__ == "__main__":
    main()
