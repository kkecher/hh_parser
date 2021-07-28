#!/usr/bin/env python3

"""
Input tests for hh_parser.py
"""
import re
import types

def test_var_type(var, var_name, var_type):
    """
    Test if type(`var`) == `var_type`.
    """
    assert isinstance(var, var_type),\
        "Expected type(%s) == %s.\n\
        Got type(%s) == %s" % (var_name, var_type, var_name, type(var))
    return ()

def test_var_len_more_than(var, var_name, var_len):
    """
    Test if len(`var`) > var_len
    """
    assert len(var) > var_len,\
        "Expected len(%s) > %d.\n\
        Got len(%s) == %d" % (var_name, var_len, var_name, len(var))
    return ()

def test_get_areas_headers(headers):
    """
    Test `headers` input.
    """
    test_var_type(headers, "headers", dict)


    keys = headers.keys()
    values = headers.values()
    for key in keys:
        test_var_type(key, "key", str)

    for value in values:
        test_var_type(value, "value", str)
    return ()

def test_get_areas(headers):
    """
    Combine all tests for `get_areas`.
    """
    test_get_areas_headers(headers)
    return ()


def test_write_to_file_file_name(file_name):
    """
    Test `file_name` input.
    """
    test_var_type(file_name, "file_name", str)
    test_var_len_more_than(file_name, "file_name", 0)
    return ()

def test_json_data_type(json_data):
    """
    Test `json_data` input.
    """
    test_var_type(json_data, "json_data", (dict, list))
    return ()

def test_write_to_file(file_name, json_data):
    """
    Combine all tests for `write_to_file`.
    """
    test_write_to_file_file_name(file_name)
    test_json_data_type(json_data)
    return ()

def test_database_name(database):
    """
    Test `database` input.
    """
    test_var_type(database, "database", str)
    test_var_len_more_than(database, "database", 0)
    return ()

def test_table_name(table):
    """
    Test `table` input.
    """
    test_var_type(table, "table", str)
    test_var_len_more_than(table, "table", 0)
    assert table.isalnum(),\
        "Only numbers and letters are allowed in table name.\n\
        Got `table` = %s" % table
    return ()

def test_create_table_columns(database, table, columns):
    """
    Combine all tests for `create_table`.
    """
    test_database_name(database)
    test_table_name(table)

    test_var_type(columns, "columns", list)
    test_var_len_more_than(columns, "columns", 0)

    for column in columns:
        test_var_type(column, "column", str)
        test_var_len_more_than(column, "column", 0)
    return ()

def test_get_table_columns_names(database, table):
    """
    Combine all tests for `get_table_columns_names`.
    """
    test_database_name(database)
    test_table_name(table)
    return ()

def test_insert_into_table(database, table, data):
    """
    Combine all tests for `insert_into_table`.
    """
    test_database_name(database)
    test_table_name(table)
    test_var_type(data, "data", dict)
    test_var_len_more_than(data, "data", 0)

    for key, value in data.items():
        test_var_type(key, "key", str)
        test_var_len_more_than(key, "key", 0)
        test_var_type(value, "value", (str, type(None)))
        if type(value) != type(None):
            test_var_len_more_than(value, "value", 0)
    return ()

def test_write_areas_to_database(database, table, areas_generator):
    """
    Combine all tests for `write_areas_to_database`.
    """
    test_database_name(database)
    test_table_name(table)

    assert isinstance(areas_generator, types.GeneratorType),\
        "Expected type(areas_generator) == generator,\n\
        Got type(areas_generator) == %s" % type(areas_generator)
    return ()

def test_select_areas_by_name(database, table, area_names):
    """
    Valid area name must follow all these rules:
    - can contain only Russian letters
    - can contain numbers
    - can contain characters: - ́ ’ , ( ) . and whitespaces.
    - 1 <= length <= 100 characters
    """
    test_database_name(database)
    test_table_name(table)
    test_var_type(area_names, "area_names", list)
    test_var_len_more_than(area_names, "area_names", 0)

    regex = re.compile(r'[0-9ёЁа-яА-Я-́’,()\s\.]')
    for area_name in area_names:
        test_var_type(area_name, "area_name", str)
        test_var_len_more_than(area_name, "area_name", 0)    

        assert len(area_name) <= 100,\
        "Expected len(area_name) <= 100\n\
        Got len(area_name) == %d" % len(area_name)

        forbidden_characters = regex.sub("", area_name)
        assert forbidden_characters == "",\
        "Forbidden characters `%s` in `area_name`.\n\n\
        Valid area name must follow all these rules:\n\
        - can contain only Russian letters\n\
        - can contain numbers\n\
        - can contain characters: - ́ ’ , ( ) . and whitespaces." % forbidden_characters
    return ()
