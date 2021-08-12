#!/usr/bin/env python3.6

"""
Get and filter geo areas.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

import sqlite3
import time

import requests

from shared import (
    create_table,
    create_table_columns,
    get_table_columns_names,
    read_config,
    write_to_database,
    write_to_file
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

config = read_config()
headers = config["headers"]
database = config["database"]
areas_file = config["areas_file"]
areas_table = config["areas_table"]

def load_areas(headers):
    """
    Get json with geo (countries, regions, cities) and their ids.
    We'll write this json to sqlite database to search by areas.
    """
    print ("\n\n    Loading areas from hh...")
    in_tests.test_request_headers(headers)

    url = "https://api.hh.ru/areas"
    response = requests.get(url, headers=headers)
    areas = response.json()
    out_tests.test_load_areas(response, areas)
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
        print (f"\n\n    Unhandled data type: {type(areas)}\n\n")
        raise TypeError
    
def write_areas_to_database(database, table, areas_generator):
    """
    Iterate over areas generator and fill the table.
    """
    print (f"\n\n    Writing geo areas to `{database} > {table}`...")
    in_tests.test_database_name(database)
    in_tests.test_table_name(table)
    in_tests.test_is_generator(generator)

    create_table(database, table,\
        ["id INT NOT NULL PRIMARY KEY",\
         "parent_id INT",\
         "name TEXT NOT NULL"
        ])
    
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
    print ("\n\nSearching geo areas...")
    in_tests.test_database_name(database)
    in_tests.test_table_name(table)
    in_tests.test_area_names(names)

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"SELECT * FROM {table} WHERE name LIKE ?"
    not_found, found, found_ids = set(), set(), set()

    for name in names:
        query_name = f"%{name.lower()}%"
        cursor.execute(query, [query_name])
        found_areas = cursor.fetchall()
        if not found_areas:
            not_found.add(name)
        else:
            for found_area in found_areas:
                found.add(found_area)
                found_ids.add(found_area[0])
    cursor.close()
    connection.close()
    out_tests.test_select_areas_by_name(not_found, found, found_ids)
    return (not_found, found, found_ids)

def clean_area_children(found, found_ids):
    """
    If `select_areas_by_name` returned some areas
    which are children of others, this function will remove such children areas.

    `found` ==  set of tuples with structure {(`id`, `parent_id`, `name`)}.
    `found_ids` == set of ints.
    """
    in_tests.test_clean_area_children(found, found_ids)
    cleaned, cleaned_ids = set(), set()
    duplicated = set()

    for area in found:
        parent_id = int(area[1])
        if parent_id not in found_ids:
            cleaned.add(area)
            cleaned_ids.add(area[0])
        else:
            duplicated.add(area)
    out_tests.test_clean_area_children(found, cleaned, duplicated)
    return (cleaned, cleaned_ids)

def get_user_inputs():
    """
    Ask user to input geo areas.
    """
    print ("\nHello! Enter geo regions where you'd like to search vacancies. \
I can understand:\n\
    1. Only Russian letters, numbers, characters - ́ ’ , ( ) . and spaces.\n\
    2. Name length up to 100 characters.\n\
    3. Incomplete names are fine too.\n\
    4. I can search many regions. Just press `Enter` to type a new one.\n\
    5. Double press `Enter` to finish.\n\n")
    user_areas = []
    user_area = True
    while True:
        try:
            if user_areas:
                print (f"\n    {user_areas}")
            user_area = input (f"    Your region: ")
            if not user_area:
                break
            in_tests.test_area_names([user_area])
            user_areas.append(user_area)
        except AssertionError:
            print (f"    INVALID NAME: {user_area}. Try again...\n")
    out_tests.test_get_user_inputs(user_areas)
    return (user_areas)

def search_user_areas(database, areas_table):
    """
    Search names and ids for user areas and confirm results.
    """
    in_tests.test_database_name(database)
    in_tests.test_table_name(areas_table)
    
    while True:
        user_areas = get_user_inputs()
        not_found, found, found_ids = \
            select_areas_by_name(database, areas_table, user_areas)
        print (f"\n\n{30*'-'}\n    NOT found areas:\n{30*'-'}")
        for area in not_found:
            print (f"    {area}")
        print (f"\n\n{30*'-'}\n    Found areas:\n{30*'-'}")
        for area in found:
            print (f"    {area[2]}")
        user_confirm = "_"
        print ()
        while (user_confirm not in "yn"):
            user_confirm = input ("    Looks good? (press `y` or `n`): ")
        if user_confirm == "y":
            break
        elif user_confirm == "n":
            print ("\n\n    Ok. Let's try again...\n")
            continue
        else:
               print ("\n\n    Unhandled user_confirm: {user_confirm}\n\n")
               raise ValueError
    return (found, found_ids)

def get_areas(
        headers=headers, database=database, \
        areas_table=areas_table, areas_file=areas_file):
    """
    Load areas from hh, save them to file (for debugging) and database.
    """
    print ("\n\nGetting areas from hh. It can take up to several minutes...\n\n")
    in_tests.test_request_headers(headers)
    in_tests.test_database_name(database)
    in_tests.test_table_name(ares_table)
    in_tests.test_write_to_file_file_name(file_name)
    
    time.sleep(4)
    Path("./data/").mkdir(parents=True, exist_ok=True)
    areas = load_areas(headers)
    write_to_file(areas_file, areas)
    write_areas_to_database(
        database, areas_table, create_areas_generator(areas))
    return ()
