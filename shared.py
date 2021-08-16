#!/usr/bin/env python3

"""
Shared functions for hh_parser.
"""

from pathlib import Path
import json
import sqlite3
import datetime
import yaml

from tests.output_tests import test_is_file_exists as is_file_exists
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def write_to_file(file_name, json_data):
    """
    Write json to file.
    This function is for debugging purpose only.
    """
    in_tests.test_write_to_file(file_name, json_data)
    print(f"    Writing json to `{file_name}`...")

    try:
        Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        print (f"I don't have permission to create {file_name}.\n\
Try to change {file_name} var value in `config.yaml` file or just solve this.")
    with open (file_name, "w") as f:
        f.write(json.dumps(json_data, indent=4, ensure_ascii=False))
    out_tests.test_is_file_exists(file_name)
    return ()

def create_table(database, table, columns):
    """
    Create table at database.
    `columns` == list of strings with columns params
    """
    in_tests.test_create_table_columns(database, table, columns)
    print (f"    Creating table `{table}` at `{database}`...")

    try:
        Path(database).parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        print (f"I don't have permission to create {database}.\n\
Try to change {database} var value in `config.yaml` file or just solve this.")
    query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
    out_tests.test_create_table_columns(
        get_table_columns_names(database, table), columns)
    return ()

def get_table_columns_names(database, table):
    """
    Get table column names.
    """
    in_tests.test_get_table_columns_names(database, table)
    print (f"    Getting `{database} > {table}` column names...")

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
        get_table_columns_names(database, table), columns)
    return ()

def write_to_database(database, table, data):
    """
    Insert or replace data in table at database.
    `data` == dict of query {key: value}
    """
    in_tests.test_write_to_database_from_dict(database, table, data)
    print (f"    Insert or update data in `{database} > {table}`...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    counter = 1
    query_columns = ", ".join(data.keys())
    query_values = f"{'?, ' * len(data)}"[:-2]
    query = \
f"INSERT OR REPLACE INTO {table} ({query_columns}) VALUES ({query_values});"
    cursor.execute(query, list(data.values()))
    connection.commit()
    database_changes = connection.total_changes
    cursor.close()
    connection.close()
    out_tests.test_write_to_database(database_changes, counter)
    return (database_changes)

def is_table_exists(database, table):
    """
    Check if table exists.
    """
    in_tests.test_database_name(database)
    in_tests.test_table_name(table)

    if is_file_exists(database):
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        query = \
f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        cursor.execute(query)
        is_table_exists = cursor.fetchone()
        cursor.close()
        connection.close()
        return (is_table_exists)
    return (False)

def set_is_sent_1(database, vacancies_table, vacancy_id):
    """
    Change `is_sent` value to 1 in `vacancy_id` row.
    """
    in_tests.test_database_name(database)
    in_tests.test_table_name(vacancies_table)
    in_tests.test_var_type(vacancy_id, "vacancy_id", int)

    current_time = datetime.datetime.now().astimezone().replace(
        microsecond=0, tzinfo=None).isoformat()
    print (
        f"    [{current_time}] Set `is_sent`=1 in vacancy id={vacancy_id}...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    counter = 1
    query = \
f"UPDATE {vacancies_table} SET is_sent = 1 WHERE id = {vacancy_id}"
    cursor.execute(query)
    connection.commit()
    database_changes = connection.total_changes
    cursor.close()
    connection.close()
    out_tests.test_write_to_database(database_changes, counter)
    return (database_changes)
