#!/usr/bin/env python3

"""
Shared functions for hh_parser.
"""

from pathlib import Path
import json
import sqlite3
import yaml

from tests.output_tests import test_is_file_exists as is_file_exists
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def read_config(config_path="config.yaml"):
    """
    """
    in_tests.test_read_config(config_path)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    out_tests.test_config(config)
    return (config)

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
Try to change {file_name} var in `config.yaml` file or just solve this.")
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
Try to change {database} var in `config.yaml` file or just solve this.")
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
