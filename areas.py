#!/usr/bin/env python3.6

"""
Get and filter geo areas.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

import sqlite3
import time
import types

import requests

from shared import (
    create_table,
    create_table_columns,
    get_table_columns_names,
    write_to_database,
    write_to_file
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def load_areas(config):
    """
    Get json with geo (countries, regions, cities) and their ids.
    We'll write this json to sqlite database to search by areas.
    """
    headers = config["headers"]
    in_tests.test_request_headers(headers)
    print ("\n\n    Loading areas from hh...")

    url = "https://api.hh.ru/areas"
    response = requests.get(url, headers=headers)
    areas = response.json()
    out_tests.test_load_areas(response, areas)
    return (areas)

def create_areas_generator(areas):
    """
    Create areas iterator to flatten multilevel json
    (countries > regions > cities) into single-level database table.
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

def get_areas(config):
    """
    Load areas from hh, save them to file (for debugging) and database.
    """
    areas_file = config["tables"]["areas_file"]
    print ("\n\nGetting areas from hh. \
It is one time operation and can take up to several minutes...\n")
    time.sleep(5)

    areas = load_areas(config)
    write_to_file(areas_file, areas)
    write_areas_to_database(config, create_areas_generator(areas))
    return ()

def write_areas_to_database(config, areas_generator):
    """
    Iterate over areas generator and fill the table.
    """
    database = config["database"]
    table = config["tables"]["areas_table"]
    in_tests.test_database_name(database)
    in_tests.test_table_name(table)
    in_tests.test_var_type(
        areas_generator, "areas_generator", types.GeneratorType)
    print (f"\n\n    Writing geo areas to `{database} > {table}`...")

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

def get_user_inputs():
    """
    Ask user to input geo areas.
    """
    print ("\n[Hello! Enter geo regions where you'd like to search vacancies. \
I can understand:\n\
    1. Only Russian letters, numbers, characters - ́ ’ , ( ) . and spaces.\n\
    2. Name length up to 100 characters.\n\
    3. Incomplete names are fine too.\n\
    4. I can search many regions. Just press `Enter` to type a new one.\n\
    5. Double press `Enter` to finish.]\n\n")
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
        if len(user_areas) == 0:
            not_found = set()
            found = set()
            found_ids = set()
        else:
            not_found, found, found_ids = \
                select_areas_by_name(database, areas_table, user_areas)
        print (f"\n\n  {30*'-'}\n    NOT found regions:\n  {30*'-'}")
        for area in not_found:
            print (f"    {area}")
        print (f"\n\n  {30*'-'}\n    Found regions:\n  {30*'-'}")
        if len(found) == 0:
            print (
"    No found regions so I'll get vacancies from all regions.")
        else:
            for area in found:
                print (f"    {area[2]}")
        user_confirm = ""
        print ()
        while (user_confirm not in "yn" or not user_confirm):
            user_confirm = input ("  Looks good? (press `y` or `n`): ")
        if user_confirm == "y":
            break
        elif user_confirm == "n":
            print ("\n\n    Ok. Let's try again...\n")
            continue
        else:
               print ("\n\n    Unhandled user_confirm: {user_confirm}\n\n")
               raise ValueError
    cleaned, cleaned_ids = clean_area_children(
        found, found_ids)
    return (list(cleaned_ids))

def select_areas_by_name(database, table, names):
    """
    Select geo areas by name to get their ids.
    `names` == list of names for search.
    """
    in_tests.test_database_name(database)
    in_tests.test_table_name(table)
    in_tests.test_area_names(names)
    print ("\n\nSearching geo areas...")

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

def check_if_area_id_is_in_areas_table(config, area_id):
    """
    1. Check if area_id is in `areas_table`.
    2. If not -> update `areas_table` by calling `get_areas`
    3. Check if area_id is in `areas_table`.
    4. If yes -> return True, else -> raise
    """
    database = config["database"]
    areas_table = config["tables"]["areas_table"]
    in_tests.test_database_name(database)
    in_tests.test_table_name(areas_table)
    in_tests.test_var_type(area_id, "area_id", int)
    print (f"    Checking if area_id is in {database} > {areas_table}...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"SELECT EXISTS (SELECT 1 FROM {areas_table} WHERE id == {area_id})"
    is_area_id_in_areas_table = cursor.execute(query).fetchall()[0][0]
    if not is_area_id_in_areas_table:
        get_areas(config)
        is_area_id_in_areas_table = cursor.execute(query).fetchall()[0][0]
    if not is_area_id_in_areas_table:
        cursor.close()
        connection.close()
        print (f"\n\n    I've updated areas but couldn't find id == \
{area_id} in {areas_table}.\n\n")
        raise ValueError
    else:
        cursor.close()
        connection.close()
        return (True)
