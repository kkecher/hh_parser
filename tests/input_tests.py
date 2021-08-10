#!/usr/bin/env python3

"""
Input tests for hh_parser.
"""
import re
import types

# Common tests
def test_var_type(var, var_name, var_type):
    """
    Test if type(`var`) == `var_type`.
    """
    assert isinstance(var, var_type), \
        "Expected type(%s) == %s.\n\
        Got type(%s) == %s" % (var_name, var_type, var_name, type(var))
    return ()

def test_var_len_more_than(var, var_name, var_len):
    """
    Test if len(`var`) > var_len
    """
    assert len(var) > var_len, \
        "Expected len(%s) > %d.\n\
        Got len(%s) == %d" % (var_name, var_len, var_name, len(var))
    return ()

def test_request_headers(headers):
    """
    Test for input request headers.
    """
    test_var_type(headers, "headers", dict)

    keys = headers.keys()
    values = headers.values()
    for key in keys:
        test_var_type(key, "key", str)

    for value in values:
        test_var_type(value, "value", str)
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

    regex = re.compile(r"[0-9a-zA-Z_]")
    forbidden_characters = regex.sub("", table)
    assert forbidden_characters == "", \
        "Only English letters, numbers and underscore are allowed in table name.\n\
        Got `table` == %s" % table
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

def test_write_to_database_from_dict(database, table, data):
    """
    Combine all tests for `write_to_database_from_dict`.
    """
    test_database_name(database)
    test_table_name(table)
    test_var_type(data, "data", dict)
    test_var_len_more_than(data, "data", 0)

    for key, value in data.items():
        test_var_type(key, "key", str)
        test_var_len_more_than(key, "key", 0)
        test_var_type(value, "value", (str, int, float, type(None)))
        if type(value) not in [type(None), bool, int, float]:
            test_var_len_more_than(value, "value", 0)
    return ()

def test_write_to_database_from_generator(database, table, generator):
    """
    Tests for writing to database from generator.
    """
    test_database_name(database)
    test_table_name(table)

    assert isinstance(generator, types.GeneratorType), \
        "Expected type(generator) == generator,\n\
        Got type(generator) == %s" % type(generator)
    return ()

def test_select_by_name(database, table, names):
    """
    Valid name must follow all these rules:
    - can contain numbers
    - can contain characters: - ́ ’ , ( ) . and whitespaces.
    - 1 <= length <= 100 characters
    """
    test_database_name(database)
    test_table_name(table)
    test_var_type(names, "names", list)
    test_var_len_more_than(names, "names", 0)

    regex = re.compile(r"[0-9a-zA-ZёЁа-яА-Я-́’,()\s\.]")
    for name in names:
        test_var_type(name, "name", str)
        test_var_len_more_than(name, "name", 0)

        assert len(name) <= 100, \
        "Expected len(name) <= 100\n\
        Got len(name) == %d" % len(name)

        forbidden_characters = regex.sub("", name)
        assert forbidden_characters == "", \
        "Forbidden characters `%s` in `name`.\n\n\
        Valid name must follow all these rules:\n\
        - can contain numbers\n\
        - can contain characters: - ́ ’ , ( ) . and whitespaces." % forbidden_characters
    return ()

def test_rename_json_to_database_key(key):
    """
    Tests for `rename_json_to_database_key`.
    """
    test_var_type(key, "key", str)
    test_var_len_more_than(key, "key", 0)
    test_table_name(key)
    return ()

# `get_areas` tests
def test_clean_area_children(found_names, found_ids):
    """
    Combine all tests for `clean_area_children`.
    """

    test_var_type(found_ids, "found_ids", set)

    for area_name in found_names:
        test_var_type(area_name, "area_name", tuple)

    for area_id in found_ids:
        test_var_type(area_id, "area_id", int)
    return ()


# `get_vacancies` tests
def test_get_vacancies(headers, filters):
    """
    Tests for `get_vacancies`.
    """
    test_request_headers(headers)
    test_var_type(filters, "filters", dict)

    keys, values = filters.keys(), filters.values()
    for key in keys:
        test_var_type(key, "key", str)
    for value in values:
        test_var_type(value, "value", (str, list))
    return ()

# ‘send_to_telegram’ tests
def test_filter_vacancies(
        database, vacancies_table, areas_table, send_columns, filters, \
        filters_query_part, inverse_filters_query_part):
    """
    Tests for `filter_vacancies`.
    """
    test_database_name(database)
    test_table_name(vacancies_table)
    test_table_name(areas_table)

    test_var_type(send_columns, "send_columns", list)
    test_var_len_more_than(send_columns, "send_columns", 0)
    for column in send_columns:
        test_var_type(column, "column", str)
        test_var_len_more_than(column, "column", 0)

    test_var_type(filters, "filters", dict)
    test_var_len_more_than(filters, "filters", 0)
    for key, value in filters.items():
        test_var_type(key, "key", str)
        test_var_len_more_than(key, "key", 0)
        test_var_type(value, "value", list)
        test_var_len_more_than(value, "value", 0)

        for param in value:
            test_var_type(param, "param", str)
            test_var_len_more_than(param, "param", 0)

    test_var_type(filters_query_part, "filters_query_part", str)
    test_var_len_more_than(filters_query_part, "filters_query_part", 0)

    test_var_type(inverse_filters_query_part, "inverse_filters_query_part", str)
    test_var_len_more_than(inverse_filters_query_part, "inverse_filters_query_part", 0)
    return ()

def test_format_filters_to_query(filters):
    """
    Tests for `format_filters_to_query`.
    """
    test_var_type(filters, "filters", dict)
    test_var_len_more_than(filters, "filters", 0)
    for key, value in filters.items():
        test_var_type(key, "key", str)
        test_var_len_more_than(key, "key", 0)
        test_var_type(value, "value", list)
        test_var_len_more_than(value, "value", 0)

        for param in value:
            test_var_type(param, "param", str)
            test_var_len_more_than(param, "param", 0)
    return ()

def test_write_filtered_vacancies_to_file(filters, clean_vacancies, dirty_vacancies):
    """
    Tests for `write_filteres_vacancies_to_file`.
    """
    test_var_type(filters, "filters", dict)
    test_var_len_more_than(filters, "filters", 0)
    for key, value in filters.items():
        test_var_type(key, "key", str)
        test_var_len_more_than(key, "key", 0)
        test_var_type(value, "value", list)
        test_var_len_more_than(value, "value", 0)
        for param in value:
            test_var_type(param, "param", str)
            test_var_len_more_than(param, "param", 0)

    test_var_type(clean_vacancies, "clean_vacancies", list)
    test_var_type(dirty_vacancies, "dirty_vacancies", list)

    for clean_vacancy in clean_vacancies:
          test_var_type(clean_vacancy, "clean_vacancy", dict)
          test_var_len_more_than(clean_vacancy, "clean_vacancy", 0)
          for key, value in clean_vacancy.items():
                test_var_type(key, "key", str)
                test_var_len_more_than(key, "key", 0)
                test_var_type(value, "value", (int, str, type(None)))

    for dirty_vacancy in dirty_vacancies:
          test_var_type(dirty_vacancy, "dirty_vacancy", dict)
          test_var_len_more_than(dirty_vacancy, "dirty_vacancy", 0)
          for key, value in dirty_vacancy.items():
                test_var_type(key, "key", str)
                test_var_len_more_than(key, "key", 0)
                test_var_type(value, "value", (int, str, type(None)))
    return ()

def test_replace_specials_to_underscore(string):
    """
    Tests for `replace_specials_to_underscore`.
    """
    test_var_type(string, "string", str)
    return ()

def test_format_values(data):
    """
    Tests for `format_values`.
    """
    test_var_type(data, "data", (str, int, float, type(None)))
    return ()

def test_send_to_telegram(vacancies, chat_id, token):
    """
    Tests for `send_to_telegram`.
    """
    test_var_type(vacancies, "vacancies", list)    
    for vacancy in vacancies:
          test_var_type(vacancy, "vacancy", dict)
          test_var_len_more_than(vacancy, "vacancy", 0)
          for key, value in vacancy.items():
                test_var_type(key, "key", str)
                test_var_len_more_than(key, "key", 0)
                test_var_type(value, "value", (int, str, type(None)))

    test_var_type(chat_id, "chat_id", int)
    test_var_type(token, "token", str)
    test_var_len_more_than(token, "token", 0)
    return ()
