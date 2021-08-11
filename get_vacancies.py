#!/usr/bin/env python3.6

"""
Get hh vacancies.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

import json
import sqlite3
import time

import requests

from common_functions import (
    create_table,
    create_table_columns,
    get_table_columns_names,
    read_config,
    write_to_database,
    write_to_file
)
from get_areas import get_hh_areas
import tests.input_tests as in_tests
import tests.output_tests as out_tests

config = read_config()
headers = config["headers"]
database = config["database"]
vacancies_file = config["vacancies_file"]
vacancies_table = config["vacancies_table"]
areas_table = config["areas_table"]

def load_vacancies(headers, filters):
    """
    Get vacancies under `filters`.
    """
    print ("Getting vacancies...")
    print (f"headers = {headers}")
    print (f"filters = {filters}")
    in_tests.test_load_vacancies(headers, filters)

    url = "https://api.hh.ru/vacancies"
    response = requests.get(url, headers=headers, params=filters)
    vacancies = response.json()
    out_tests.test_load_vacancies(response, vacancies, filters)
    return (vacancies)

def create_vacancies_generator(vacancies, parent_key = ""):
    """
    Create vacancies iterator to flatten multilevel json
    into one-level database table.
    """
    in_tests.test_json_data_type(vacancies)

    if isinstance(vacancies, dict):
        for key, value in vacancies.items():
            if isinstance(value, (dict, list)):
                yield from create_vacancies_generator(value, parent_key+key+"_")
            else:
                yield (parent_key+key, value)
    elif isinstance(vacancies, list):
        for item in vacancies:
            yield from create_vacancies_generator(item, parent_key)
    else:
        print ()
        print ("Unhandled data type: ", type(vacancies))
        print ()
        raise TypeError

def create_core_vacancies_tables(database):
    """
    Create core vacancies' tables.
    """
    in_tests.test_create_core_vacancies_tables(database)
    
    create_table(database, "streets",\
        ["area_id INTEGER NOT NULL",\
         "city_name TEXT",\
         "street_name TEXT",\
         "PRIMARY KEY (area_id, city_name, street_name)",\
         f"FOREIGN KEY (area_id) REFERENCES {areas_table} (id)\
         ON UPDATE CASCADE ON DELETE RESTRICT"
         ])

    create_table(database, "metro_stations",\
        ["station_id TEXT NOT NULL PRIMARY KEY",\
         "station_name TEXT NOT NULL",\
         "line_name TEXT NOT NULL",\
         "station_lat TEXT",\
         "station_lng TEXT"
         ])

    create_table(database, "employers",\
    ["id INTEGER NOT NULL PRIMARY KEY",\
     "name TEXT NOT NULL",\
     "url TEXT NOT NULL",\
     "alternate_url TEXT NOT NULL",\
     "logo_url_original TEXT",\
     "logo_url_240 TEXT",\
     "logo_url_90 TEXT",\
     "vacancies_url TEXT NOT NULL",\
     "is_trusted INTEGER"
     ])

    create_table(database, "vacancies_metro_stations",\
    ["vacancy_id INTEGER NOT NULL",\
     "metro_station_id TEXT NOT NULL",\
    "PRIMARY KEY (vacancy_id, metro_station_id)",\
    f"FOREIGN KEY (vacancy_id) REFERENCES {vacancies_table} (id)\
    ON UPDATE CASCADE ON DELETE CASCADE", \
    f"FOREIGN KEY (metro_station_id) REFERENCES metro_stations (station_id)\
    ON UPDATE CASCADE ON DELETE CASCADE"
     ])

    create_table(database, vacancies_table,\
    ["id INTEGER NOT NULL PRIMARY KEY",\
     "name TEXT",\
     "area_id INTEGER",\
     "address_city  TEXT",\
     "address_street TEXT",\
     "employer_id INT",\
     "alternate_url TEXT",\
     "salary_from INT",\
     "salary_to INT",\
     "salary_currency TEXT",\
     "salary_gross INT",\
     "snippet_responsibility TEXT",\
     "snippet_requirement TEXT",\
     "schedule_name TEXT",\
     "working_time_intervals_name TEXT",\
     "working_time_modes_name TEXT",\
     f"FOREIGN KEY (area_id) REFERENCES {areas_table} (id)\
     ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (area_id, address_city, address_street)\
     REFERENCES streets (area_id, city_name, street_name)\
     ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (employer_id) REFERENCES employers (id)\
     ON UPDATE CASCADE ON DELETE RESTRICT"
     ])
    return ()
    
def check_if_area_id_is_in_areas_table(database, milestone_cache, areas_table=areas_table):
    """
    1. Check if area_id is in `areas_table`.
    2. If not -> update `areas_table` by calling `get_hh_areas`
    3. Check if area_id is in `areas_table`.
    4. If yes -> return key, else -> raise
    """
    print (f"    Checking if area_id is in {database} > {areas_table}...")
    in_tests.test_write_to_database_from_dict(
        database, areas_table, milestone_cache)

    area_id = milestone_cache["area_id"]
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"SELECT EXISTS (SELECT 1 FROM {areas_table} WHERE id == {area_id})"
    is_area_in_areas_table = cursor.execute(query).fetchall()[0][0]
    if not is_area_in_areas_table:
        get_hh_areas()
        is_area_in_areas_table = cursor.execute(query).fetchall()[0][0]
    if not is_area_in_areas_table:
        cursor.close()
        connection.close()
        print ()
        print (f"    I've updated areas but can't find id == \
{area_id} in {areas_table}. ")
        print ()
        raise ValueError
    else:
        cursor.close()
        connection.close()
        return (True)

def write_street_to_streets_table(database, milestone_cache):
    """
    Write or update `aread_id - city_name - street_name` row in `streets` table.
    """
    streets_table = "streets"
    print (f"    Writing street to {database} > {streets_table}...")
    in_tests.test_write_to_database_from_dict(
        database, streets_table, milestone_cache)

    counter = 1
    database_changes = 0
    row = {
        "area_id": milestone_cache["area_id"],
        "city_name": milestone_cache["address_city"],
        "street_name": milestone_cache["address_street"]
        }
    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    database_changes += write_to_database(database, streets_table, row)
    cursor.close()
    connection.close()
    out_tests.test_write_to_database(database_changes, counter)
    return ()

def write_station_to_metro_stations_table(database, milestone_cache):
    """
    Write or update `station_id - station_name - line_name -
    station_lat - station_lng` row in `metro_stations` table.
    """
    metro_stations_table = "metro_stations"
    print (f"    Writing metro_station to {database} > \
{metro_stations_table}...")
    in_tests.test_write_to_database_from_dict(
        database, metro_stations_table, milestone_cache)

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    counter = 2
    database_changes = 0
    station_row = {
        "station_id": milestone_cache["address_metro_stations_station_id"],
        "station_name": milestone_cache["address_metro_stations_station_name"],
        "line_name": milestone_cache["address_metro_stations_line_name"],
        "station_lat": milestone_cache["address_metro_stations_lat"],
        "station_lng": milestone_cache["address_metro_stations_lng"]
    }
    database_changes += write_to_database(
        database, metro_stations_table, station_row)

    vacancy_station_join_table = "vacancies_metro_stations"
    vacancy_station_join_row = {
        "vacancy_id": milestone_cache["id"],
        "metro_station_id": milestone_cache["address_metro_stations_station_id"]
    }
    database_changes += write_to_database(
        database, vacancy_station_join_table, vacancy_station_join_row)
    cursor.close()
    connection.close()
    out_tests.test_write_to_database(database_changes, counter)
    return ()

def write_employer_to_employeers_table(database, milestone_cache):
    """
    Write or update employer row in `employers` table.
    """
    employers_table = "employers"
    print (f"    Writing employer to {database} > {employers_table}...")
    in_tests.test_write_to_database_from_dict(
        database, employers_table, milestone_cache)

    counter = 1
    database_changes = 0
    row = {
        "id": milestone_cache["employer_id"],
        "name": milestone_cache["employer_name"],
        "url": milestone_cache["employer_url"],
        "alternate_url": milestone_cache["employer_alternate_url"],
        "logo_url_original": milestone_cache["employer_logo_urls_original"],
        "logo_url_240": milestone_cache["employer_logo_urls_240"],
        "logo_url_90": milestone_cache["employer_logo_urls_90"],
        "vacancies_url": milestone_cache["employer_vacancies_url"],
        "is_trusted": milestone_cache["employer_trusted"]
        }
    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    database_changes += write_to_database(database, employers_table, row)
    cursor.close()
    connection.close()
    out_tests.test_write_to_database(database_changes, counter)
    return ()

def write_vacancies_to_database(database, table, vacancies_generator):
    """
    Iterate over vacancies generator and fill `vacancies` table.
    """
    print (f"Writing vacancies to `{database} > {table}`...")
    in_tests.test_write_to_database_from_generator(
        database, table, vacancies_generator)

    create_core_vacancies_tables(database)
    
    columns_names = get_table_columns_names(database, table)
    vacancy = {}
    vacancy_counter = 0
    database_changes = 0

    # Dict for accumulation of milestone data
    milestone_cache = {
        "area_id": None,
        "address_city": None,
        "address_street": None,
        "address_metro_stations_station_id": None,
        "address_metro_stations_station_name": None,
        "address_metro_stations_line_name": None,
        "address_metro_stations_lat": None,
        "address_metro_stations_lng": None,
        "employer_id": None,
        "employer_name": None,
        "employer_url": None,
        "employer_alternate_url": None,
        "employer_logo_urls_original": None,
        "employer_logo_urls_240": None,
        "employer_logo_urls_90": None,
        "employer_vacancies_url": None,
        "employer_trusted": None
        }

    # After each of these keys we run its connected function.
    milestone_keys = {
    "area_id": check_if_area_id_is_in_areas_table,
    "address_raw": write_street_to_streets_table,
    "address_metro_stations_lng": write_station_to_metro_stations_table,
    "employer_trusted": write_employer_to_employeers_table
    }

    # We don’t write these keys to vacancies table
    # because they are in some other one.
    skip_key = [
        "area_name",
        "area_url",
        "address_metro_station_name",
        "address_metro_line_name",
        "address_metro_station_id",
        "address_metro_line_id",
        "address_metro_lat",
        "address_metro_lng",
        "address_metro_stations_station_name",
        "address_metro_stations_line_name",
        "address_metro_stations_station_id",
        "address_metro_stations_line_id",
        "address_metro_stations_lat",
        "address_metro_stations_lng",
        "employer_name",
        "employer_url",
        "employer_alternate_url",
        "employer_logo_urls_original",
        "employer_logo_urls_90",
        "employer_logo_urls_240",
        "employer_vacancies_url",
        "employer_trusted",
    ]

    for item in vacancies_generator:
        key, value = item[0], item[1]

        # Lowercase text to have case-insensitive search.
        # `COLLATE NOCASE` doesn't work for cyrillic.
        try:
            value = value.lower()
        except AttributeError:
            pass

        if key not in columns_names and key not in skip_key:
            column = [(f"{key} TEXT")]
            create_table_columns(database, table, column)
            columns_names.append(key)

        if key != "id" or vacancy == {}:
            milestone_cache[key] = value
            if key in milestone_keys:
                key_function = milestone_keys[key]
                key_function(database=database, milestone_cache=milestone_cache)
            if key not in skip_key and value != None:
                vacancy[key] = value
        else:
            database_changes += write_to_database(database, table, vacancy)
            vacancy_counter += 1
            vacancy = {}
            milestone_cache = {
                "area_id": None,
                "address_city": None,
                "address_street": None,
                "address_metro_stations_station_id": None,
                "address_metro_stations_station_name": None,
                "address_metro_stations_line_name": None,
                "address_metro_stations_lat": None,
                "address_metro_stations_lng": None,
                "employer_id": None,
                "employer_name": None,
                "employer_url": None,
                "employer_alternate_url": None,
                "employer_logo_urls_original": None,
                "employer_logo_urls_240": None,
                "employer_logo_urls_90": None,
                "employer_vacancies_url": None,
                "employer_trusted": None
                }
            vacancy[key] = value
            milestone_cache[key] = value
    database_changes += write_to_database(database, table, vacancy)
    vacancy_counter += 1
    out_tests.test_write_to_database(database_changes, vacancy_counter)
    return ()

def get_hh_vacancies(
        headers, database, vacancies_table, vacancies_file, cleaned_ids):
    """
    Load vacancies from hh, save them to file (for debugging) and database.
    """
    filters = {"area": cleaned_ids}
    vacancies = load_vacancies(headers, filters)
    write_to_file(vacancies_file, vacancies)
    write_vacancies_to_database(
database, vacancies_table, create_vacancies_generator(vacancies["items"]))
    return ()
