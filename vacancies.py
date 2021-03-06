#!/usr/bin/env python3.6

"""
Get hh vacancies.

API resources:
https://github.com/hhru/api/blob/master/docs_eng/README.md
https://github.com/hhru/api/blob/master/docs_eng/vacancies.md
"""

from copy import deepcopy
import datetime
import math
import types

import requests

from areas import check_if_area_id_is_in_areas_table
from config import import_database_columns
from shared import (
    create_table,
    create_table_columns,
    get_table_columns_names,
    write_to_database,
    write_to_file
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def get_vacancies(config):
    """
    Load vacancies from hh, save them to file (for debugging) and database.
    """
    vacancies_file = deepcopy(config["vacancies_file"])
    headers = deepcopy(config["headers"])
    filters = deepcopy(config["url_params"])
    filters["area"] = filters["area"][-1].split("|")
    if filters["area"] == [""]:
        del filters["area"]
    if "date_from"  or "date_to" in filters:
        del filters["period"]
    filters["page"] = 0
    in_tests.test_dict_data_type(filters)
    print ("\n\nGetting vacancies from hh...")

    date_current = datetime.datetime.now().replace(microsecond=0).isoformat()
    while True:
        vacancies = load_vacancies(headers, filters)
        found_vacancies = vacancies["found"]
        if found_vacancies:
            write_vacancies_to_database(
                config, create_vacancies_generator(vacancies["items"]))
        write_to_file(vacancies_file, vacancies)
        filters["page"] += 1
        if vacancies["pages"] <= filters["page"]:
            break
    config["url_params"]["date_from"] = date_current
    import_database_columns(config)
    got_vacancies = min(found_vacancies, filters["per_page"]*filters["page"])
    if "period" in filters:
        print(f"\n\nFound: {found_vacancies} vacancies \
for period of {filters['period']} days.")
    elif "date_from" in filters:
        print(f"\n\nFound: {found_vacancies} vacancies \
from {format(filters['date_from'])}.")
    else:
        print(
            "\n\nNo `period` or `date_from` in `config.yaml > url_params`\n\n")
        raise AttributeError
    if found_vacancies:
        print(f"Got: {got_vacancies} vacancies \
({round(got_vacancies/found_vacancies*100, 2)}%)")
    else:
        print(f"Got: {got_vacancies} vacancies (0%)")
    if found_vacancies > got_vacancies:
        print(f"\nYou can get more vacancies by:\n\
    1. Scheduling parse more often.\n\
    2. Adding more filter params to `config.yaml > url_params`.\n\
    3. Changing region\n\
    4. Changing `config.yaml > url_params > period` \
(legal values in [1, 31]).\n\
        Works only if no param `date_from`: \
")
    return ()

def load_vacancies(headers, filters):
    """
    Get vacancies under `filters`.
    """
    in_tests.test_dict_data_type(headers)
    print ("    Loading vacancies from hh...")

    url = "https://api.hh.ru/vacancies"
    response = requests.get(url, headers=headers, params=filters)
    vacancies = response.json()
    out_tests.test_load_vacancies(response, vacancies, filters)
    return (vacancies)

def create_vacancies_generator(vacancies, parent_key = ""):
    """
    Create vacancies iterator to flatten multilevel json
    into single-level database table.
    """
    in_tests.test_var_type(vacancies, "vacancies", (dict, list))

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
        print (f"\n\n    Unhandled data type: {type(vacancies)}\n\n")
        raise TypeError

def create_vacancies_tables(config):
    """
    Create vacancies' tables.
    """
    database = deepcopy(config["database"])
    areas_table = deepcopy(config["tables"]["areas_table"])
    vacancies_table = deepcopy(config["tables"]["vacancies_table"])
    streets_table = deepcopy(config["tables"]["streets_table"])
    metro_stations_table = deepcopy(config["tables"]["metro_stations_table"])
    employers_table = deepcopy(config["tables"]["employers_table"])
    vacancies_metro_stations_table = \
        deepcopy(config["tables"]["vacancies_metro_stations_table"])
    in_tests.test_database_name(database)
    in_tests.test_table_name(areas_table)
    in_tests.test_table_name(vacancies_table)
    in_tests.test_table_name(streets_table)
    in_tests.test_table_name(metro_stations_table)
    in_tests.test_table_name(employers_table)
    in_tests.test_table_name(vacancies_metro_stations_table)

    create_table(database, streets_table,\
        ["area_id INTEGER NOT NULL",\
         "city_name TEXT",\
         "street_name TEXT",\
         "PRIMARY KEY (area_id, city_name, street_name)",\
         f"FOREIGN KEY (area_id) REFERENCES {areas_table} (id)\
         ON UPDATE CASCADE ON DELETE RESTRICT"
         ])

    create_table(database, metro_stations_table,\
        ["station_id TEXT NOT NULL PRIMARY KEY",\
         "station_name TEXT NOT NULL",\
         "line_name TEXT NOT NULL",\
         "station_lat TEXT",\
         "station_lng TEXT"
         ])

    create_table(database, employers_table,\
    ["id INTEGER NOT NULL PRIMARY KEY",\
     "name TEXT NOT NULL",\
     "url TEXT",\
     "alternate_url TEXT",\
     "logo_url_original TEXT",\
     "logo_url_240 TEXT",\
     "logo_url_90 TEXT",\
     "vacancies_url TEXT NOT NULL",\
     "is_trusted INTEGER"
     ])

    create_table(database, vacancies_metro_stations_table,\
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
     "address_city TEXT",\
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
     "is_sent INT NOT NULL",\
     f"FOREIGN KEY (area_id) REFERENCES {areas_table} (id)\
     ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (area_id, address_city, address_street)\
     REFERENCES streets (area_id, city_name, street_name)\
     ON UPDATE CASCADE ON DELETE RESTRICT",\
     f"FOREIGN KEY (employer_id) REFERENCES employers (id)\
     ON UPDATE CASCADE ON DELETE RESTRICT"
     ])
    return ()

def write_streets_to_streets_table(config, tables_cache):
    """
    Write `area_id > city > stree` row to `streets` table.
    """
    database = deepcopy(config["database"])
    streets_table = deepcopy(config["tables"]["streets_table"])
    in_tests.test_write_to_database_from_dict(
        database, streets_table, tables_cache)

    area_id = tables_cache["area_id"]
    city_name = tables_cache["address_city"]
    street_name = tables_cache["address_street"]

    if city_name or street_name:
        write_to_database(database, streets_table, {
            "area_id": area_id,
            "city_name": city_name,
            "street_name": street_name
        })
    return ()

def write_stations_to_metro_stations_table(config, tables_cache):
    """
    Write `station_id > station_name > line_name > station_lat > station_lng`
    row to `metro_stations` table.
    Vacancy can have several metro stations. Only the last one will be written.
    """
    database = deepcopy(config["database"])
    metro_stations_table = deepcopy(config["tables"]["metro_stations_table"])
    in_tests.test_write_to_database_from_dict(
        database, metro_stations_table, tables_cache)

    station_id = tables_cache["address_metro_stations_station_id"]
    station_name = tables_cache["address_metro_stations_station_name"]
    line_name = tables_cache["address_metro_stations_line_name"]
    station_lat = tables_cache["address_metro_stations_lat"]
    station_lng = tables_cache["address_metro_stations_lng"]

    if station_id:
        write_to_database(database, metro_stations_table, {
            "station_id": station_id,
            "station_name": station_name,
            "line_name": line_name,
            "station_lat": station_lat,
            "station_lng": station_lng
        })
    return ()

def write_to_vacancies_metro_stations_table(config, tables_cache):
    """
    Write `vacancy_id > metro_station_id` to `vacancies_metro_stations`.
    Vacancy can have several metro stations. Only the last one will be written.
    """
    database = deepcopy(config["database"])
    vacancies_metro_stations_table = \
        deepcopy(config["tables"]["vacancies_metro_stations_table"])
    in_tests.test_write_to_database_from_dict(
        database, vacancies_metro_stations_table, tables_cache)

    vacancy_id = tables_cache["id"]
    station_id = tables_cache["address_metro_stations_station_id"]

    if station_id:
        write_to_database(database, vacancies_metro_stations_table, {
            "vacancy_id": vacancy_id,
            "metro_station_id": station_id
        })
    return ()

def write_employers_to_employers_table(config, tables_cache):
    """
    Write `id > name > url > alternate_url > logo_urls_original >
    logo_urls_240 > logo_urls_90 > vacancies_url > trusted` row
    to `employers` table.
    """
    database = deepcopy(config["database"])
    employers_table = deepcopy(config["tables"]["employers_table"])
    in_tests.test_write_to_database_from_dict(
        database, employers_table, tables_cache)

    id_ = tables_cache["employer_id"]
    name = tables_cache["employer_name"]
    url = tables_cache["employer_url"]
    alternate_url = tables_cache["employer_alternate_url"]
    logo_url_original = tables_cache["employer_logo_urls_original"]
    logo_url_240 = tables_cache["employer_logo_urls_240"]
    logo_url_90 = tables_cache["employer_logo_urls_90"]
    vacancies_url = tables_cache["employer_vacancies_url"]
    is_trusted = tables_cache["employer_trusted"]

    if id_:
        write_to_database(database, employers_table, {
            "id": id_,
            "name": name,
            "url": url,
            "alternate_url": alternate_url,
            "logo_url_original": logo_url_original,
            "logo_url_240": logo_url_240,
            "logo_url_90": logo_url_90,
            "vacancies_url": vacancies_url,
            "is_trusted": is_trusted
        })
    return ()

def write_vacancies_to_database(config, vacancies_generator):
    """
    Iterate over vacancies generator and fill `vacancies` table.
    """
    database = deepcopy(config["database"])
    vacancies_table = deepcopy(config["tables"]["vacancies_table"])
    in_tests.test_database_name(database)
    in_tests.test_table_name(vacancies_table)
    in_tests.test_var_type(
        vacancies_generator, "vacancies_generator", types.GeneratorType)
    print (f"    Writing vacancies to `{database} > {vacancies_table}`...")

    create_vacancies_tables(config)

    vacancies_columns_names = get_table_columns_names(database, vacancies_table)
    vacancy = {}
    vacancy_counter = 0
    database_changes = 0

    # Accumulate non-vacances tables data.
    # Its values are reseted to None every vacancy.
    tables_cache_keys = ["id", "area_id", "address_city", "address_street", \
"address_metro_stations_station_id", "address_metro_stations_station_name", \
"address_metro_stations_line_name", "address_metro_stations_lat", \
"address_metro_stations_lng", "employer_id", "employer_name", "employer_url", \
"employer_alternate_url", "employer_logo_urls_original", \
"employer_logo_urls_240", "employer_logo_urls_90", "employer_vacancies_url", \
"employer_trusted"]
    tables_cache = dict.fromkeys(tables_cache_keys)

    # We don???t write these keys to vacancies table
    # because they are in some other one.
    skip_keys = ["area_name", "area_url", "address_metro_station_name", \
"address_metro_line_name", "address_metro_station_id", \
"address_metro_line_id", "address_metro_lat", "address_metro_lng", \
"address_metro_stations_station_name", "address_metro_stations_line_name", \
"address_metro_stations_station_id", "address_metro_stations_line_id", \
"address_metro_stations_lat", "address_metro_stations_lng", "employer_name", \
"employer_url", "employer_alternate_url", "employer_logo_urls_original", \
"employer_logo_urls_90", "employer_logo_urls_240", "employer_vacancies_url", \
"employer_trusted"
    ]

    for item in vacancies_generator:
        key, value = item[0], item[1]

        # Lowercase text to have case-insensitive search.
        # `COLLATE NOCASE` doesn't work for cyrillic.
        try:
            value = value.lower()
        except AttributeError:
            pass

        if key not in vacancies_columns_names and key not in skip_keys:
            column = [(f"{key} TEXT")]
            create_table_columns(database, vacancies_table, column)
            vacancies_columns_names.append(key)

        if key != "id" or vacancy == {}:
            tables_cache[key] = value
            if key not in skip_keys and value != None:
                vacancy[key] = value
        else:
            vacancy["is_sent"] = 0
            check_if_area_id_is_in_areas_table(
                config, int(tables_cache["area_id"]))
            write_streets_to_streets_table(config, tables_cache)
            write_stations_to_metro_stations_table(config, tables_cache)
            write_employers_to_employers_table(config, tables_cache)
            write_to_vacancies_metro_stations_table(config, tables_cache)

            database_changes += write_to_database(
                database, vacancies_table, vacancy)
            vacancy_counter += 1
            vacancy = {}
            tables_cache = dict.fromkeys(tables_cache_keys)
            vacancy[key] = value
            tables_cache[key] = value

    vacancy["is_sent"] = 0
    check_if_area_id_is_in_areas_table(
        config, int(tables_cache["area_id"]))
    write_streets_to_streets_table(config, tables_cache)
    write_stations_to_metro_stations_table(config, tables_cache)
    write_employers_to_employers_table(config, tables_cache)
    write_to_vacancies_metro_stations_table(config, tables_cache)
    database_changes += write_to_database(database, vacancies_table, vacancy)
    vacancy_counter += 1
    out_tests.test_write_to_database(database_changes, vacancy_counter)
    return ()
