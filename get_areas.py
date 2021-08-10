#!/usr/bin/env python3.6

"""
Get and filter hh areas.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

import json
import sqlite3

import requests

from common_functions import (
    HEADERS,
    DATABASE,
    write_to_file,
    create_table,
    get_table_columns_names,
    create_table_columns,
    write_to_database,
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

AREAS_FILE = "./data/areas.json"
AREAS_TABLE = "areas"

def get_areas(headers):
    """
    Get json with geo (countries, regions, cities) and their ids.
    We'll write this json to sqlite database to search by areas.
    """
    print ("Getting geo areas from hh...")
    in_tests.test_request_headers(headers)

    url = "https://api.hh.ru/areas"
    response = requests.get(url, headers=headers)
    areas = response.json()
    out_tests.test_get_areas(response, areas)
    return (areas)

def create_areas_generator(areas):
    """
    Create areas iterator to flatten multilevel json
    (countries > regions > cities) into one-level database table.
    """
    in_tests.test_json_data_type(areas)

    if isinstance(areas, dict):
        for key, value in areas.items():
            if isinstance(value, (dict, list)):
                yield from create_areas_generator(value)
            else:
                yield (key, value)
    elif isinstance(areas, list):
        for item in areas:
            yield from create_areas_generator(item)
    else:
        print ()
        print ("Unhandled data type: ", type(areas))
        print ()
        raise TypeError
    
def write_areas_to_database(database, table, areas_generator):
    """
    Iterate over areas generator and fill the table.
    """
    print (f"Writing geo areas to `{database} > {table}`...")
    in_tests.test_write_to_database_from_generator(database, table, areas_generator)

    columns_names = get_table_columns_names(database, table)
    area = {}
    areas_counter = 0
    database_changes = 0

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
            database_changes += write_to_database(database, table, area)
            areas_counter += 1
            area[key] = value
    database_changes += write_to_database(database, table, area)
    areas_counter += 1
    out_tests.test_write_to_database(database_changes, areas_counter)
    return ()

def select_areas_by_name(database, table, names):
    """
    Select geo areas by name to get their ids.
    `names` == list of names for search.
    """
    print ("Search geo areas...")
    in_tests.test_select_by_name(database, table, names)

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
    out_tests.test_select_by_name(not_found_names, found_names, found_ids)
    return (not_found_names, found_names, found_ids)

def clean_area_children(found_names, found_ids):
    """
    If `select_areas_by_name` returnes some areas
    which are children of others, this function removes such children areas.

    `found_names`: set of tuples with structure {(`id`, `parent_id`, `name`)}.
    `found_ids`: set of ints.
    """
    in_tests.test_clean_area_children(found_names, found_ids)
    cleaned_names, cleaned_ids = set(), set()
    duplicated_names, duplicated_ids = set(), set()

    for found_name in found_names:
        parent_id = int(found_name[1])
        if parent_id not in found_ids:
            cleaned_names.add(found_name)
            cleaned_ids.add(found_name[0])
        else:
            duplicated_names.add(found_name)
    out_tests.test_clean_area_children(
        found_names, cleaned_names, duplicated_names)
    return (cleaned_names, cleaned_ids)

def main():
    areas = get_areas(HEADERS)
    write_to_file(AREAS_FILE, areas)
    create_table(DATABASE, AREAS_TABLE,\
        ["id INT NOT NULL PRIMARY KEY",\
         "parent_id INT",\
         "name TEXT NOT NULL"
        ])
    write_areas_to_database(DATABASE, AREAS_TABLE, create_areas_generator(areas))
    names = [
        "моск",
        "владиво",
        "moscow",
        "nys"
    ]
    not_found_names, found_names, found_ids = \
        select_areas_by_name(DATABASE, AREAS_TABLE, names)
    print () #dd
    print (f"not_found_names = {not_found_names}") #dd
    print (f"found_names = {found_names}") #dd
    print (f"found_ids = {found_ids}") #dd
    print () #dd
    cleaned_names, cleaned_ids = clean_area_children(found_names, found_ids)
    print (f"cleaned_names = {cleaned_names}") #dd
    print (f"cleaned_ids = {cleaned_ids}") #dd

    print ()
    print ("`get_areas` done!")

if __name__ == "__main__":
    main()
