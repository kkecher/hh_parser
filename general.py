#!/usr/bin/env python3

"""
General functions for hh_parser.
"""

import json

import tests.input_tests as in_tests
import tests.output_tests as out_tests

HEADERS  = {"user-agent": "kkecher (kkecher@gmail.com)"}
DATABASE = "./data/vacancies.db"

def write_to_file(file_name, json_data):
    """
    Write json to file.
    This function is for debugging only purpose.
    """
    in_tests.test_write_to_file(file_name, json_data)
    print(f"Writing json to `{file_name}`...")

    with open (file_name, "w") as f:
        f.write(json.dumps(json_data, indent=4, ensure_ascii=False))
    out_tests.test_write_to_file(file_name)
    return ()

def create_table(database, table, columns):
    """
    Create table at database.
    `columns`: list of strings with columns params
    """
    in_tests.test_create_table_columns(database, table, columns)
    print (f"Creating table `{table}` at `{database}` database...")

    query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    columns = ("id",)
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
    out_tests.test_create_table_columns(
        database, table, get_table_columns_names, columns)
    return ()

def get_table_columns_names(database, table):
    """
    Get table column names.
    """
    in_tests.test_get_table_columns_names(database, table)
    print (f"Getting `{database} > {table}` column names...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query = "PRAGMA table_info(" + str(table) + ")"
    columns = list(cursor.execute(query))
    columns_names = [column[1] for column in columns]

    cursor.close()
    connection.close()
    out_tests.test_get_table_columns_names(columns_names)
    return (columns_names)

def get_table_columns_names(database, table):
    """
    Get table column names.
    """
    in_tests.test_get_table_columns_names(database, table)
    print (f"Getting `{database} > {table}` column names...")

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
        database, table, get_table_columns_names, columns)
    return ()

def insert_into_table(database, table, data):
    """
    Insert or replace data in table at database.
    `data` = dict of query {key: value}
    """
    in_tests.test_insert_into_table(database, table, data)
    print (f"Insert or update data in `{database} > {table}`...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    query_columns = ", ".join(data.keys())
    query_values = f"{'?, ' * len(data)}"[:-2]
    query =\
f"INSERT OR REPLACE INTO {table} ({query_columns}) VALUES ({query_values});"
    cursor.execute(query, list(data.values()))
    connection.commit()
    database_changes_count = connection.total_changes
    cursor.close()
    connection.close()
    out_tests.test_insert_into_table(database_changes_count)
    return (database_changes_count)
