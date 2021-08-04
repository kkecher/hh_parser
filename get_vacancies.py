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

from common_functions import (
    HEADERS,
    DATABASE,
    write_to_file,
    create_table,
    get_table_columns_names,
    create_table_columns,
    write_to_database
)
import get_areas
from get_areas import AREAS_TABLE
import tests.input_tests as in_tests
import tests.output_tests as out_tests

VAСANCIES_FILE = "./data/vacancies.json"
VACANCIES_TABLE = "vacancies"

def get_vacancies(headers, filters):
    """
    Get vacancies under `filters`.
    """
    print ("Getting vacancies...")
    in_tests.test_get_vacancies(headers, filters)

    url = "https://api.hh.ru/vacancies"
    response = requests.get(url, headers=headers, params=filters)
    vacancies = response.json()
    out_tests.test_get_vacancies(response, vacancies, filters)
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

def check_if_area_id_is_in_areas_table(database, milestone_cache):
    """
    1. Check if area_id is in `AREAS_TABLE`.
    2. If not -> update `AREAS_TABLE` by calling `get_areas.mail()`
    3. Check if area_id is in `AREAS_TABLE`.
    4. If yes -> return key, else -> raise
    """
    areas_table = AREAS_TABLE
    print (f"    Checking if area_id is in {database} > {areas_table}...")
    in_tests.test_write_to_database_from_dict(
        database, areas_table, milestone_cache)

    area_id = milestone_cache["area_id"]
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = f"SELECT EXISTS (SELECT 1 FROM {areas_table} WHERE id = {area_id})"
    is_area_in_areas_table = cursor.execute(query).fetchall()[0][0]
    if not is_area_in_areas_table:
        get_areas.main()
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

    columns_names = get_table_columns_names(database, table)
    vacancy = {}
    vacancy_counter = 0
    database_changes = 0

    # Dict for accumulation of milestone data
    milestone_cache = {}

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

        if key not in columns_names and key not in skip_key and value != None:
            column = [(f"{key} TEXT")]
            create_table_columns(database, table, column)
            columns_names.append(key)

        if key != "id" or vacancy == {}:
            milestone_cache[key] = value
            if key in milestone_keys:
                key_function = milestone_keys[key]
                key_function(database, milestone_cache)
            if key not in skip_key and value != None:
                vacancy[key] = value
        else:
            database_changes += write_to_database(database, table, vacancy)
            vacancy_counter += 1
            vacancy[key] = value
            milestone_cache[key] = value
    database_changes += write_to_database(database, table, vacancy)
    vacancy_counter += 1
    out_tests.test_write_to_database(database_changes, vacancy_counter)
    return ()

def main():
    filters = {"area": [1]}
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
     "address_city  TEXT", "address_street TEXT",\
     "employer_id TEXT",\
     f"FOREIGN KEY (area_id) REFERENCES {AREAS_TABLE} (id)\
    ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (area_id, address_city, address_street)\
     REFERENCES streets (area_id, city_name, street_name)\
     ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (employer_id) REFERENCES employers (id)\
    ON UPDATE CASCADE ON DELETE RESTRICT"])

    write_vacancies_to_database(
    DATABASE, VACANCIES_TABLE, create_vacancies_generator(vacancies["items"]))

    print ()
    print ('get_vacancies done!')

if __name__ == "__main__":
    main()
