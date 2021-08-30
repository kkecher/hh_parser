#!/usr/bin/env python3.6

"""
Input tests for hh_parser.
"""
import os.path
import re

# SHARED TESTS
def test_var_type(var, var_name, var_type):
    """
    Test if type(`var`) == `var_type`.
    """
    assert isinstance(var, var_type), \
        "\n\nExpected type(%s) == %s.\n\
        Got type(%s) == %s" % (var_name, var_type, var_name, type(var))
    return ()

def test_var_len_more_than(var, var_name, var_len):
    """
    Test if len(`var`) > var_len
    """
    assert len(var) > var_len, \
        "\n\nExpected len(%s) > %d.\n\
        Got len(%s) == %d" % (var_name, var_len, var_name, len(var))
    return ()

def test_var_len_equal(var, var_name, var_len):
    """
    Test if len(`var`) == var_len
    """
    assert len(var) == var_len, \
        "\n\nExpected len(%s) == %d.\n\
        Got len(%s) == %d" % (var_name, var_len, var_name, len(var))
    return ()

def test_dict_data_type(dict_data):
    """
    Test dict data input.
    """
    test_var_type(dict_data, "dict_data", dict)
    test_var_len_more_than(dict_data, "dict_data", 0)

    for key, value in dict_data.items():
        test_var_type(key, "key", str)
        test_var_len_more_than(key, "key", 0)
        test_var_type(value, "value", (str, int, float, list, dict))
    return ()

def test_list_data_type(list_data):
    """
    Test list data input.
    """
    test_var_type(list_data, "list_data", list)

    for item in list_data:
        test_var_type(item, "item", (str, int, float))
    return ()

def test_is_file_exists(file_name):
      """
      Test if data was written to file.
      """
      assert os.path.isfile(file_name), \
            "\n\nfile `%s` was not created or doesn't exists." % file_name
      return (True)

def test_write_to_file_file_name(file_name):
    """
    Test `file_name` input.
    """
    test_var_type(file_name, "file_name", str)
    test_var_len_more_than(file_name, "file_name", 0)
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
"\n\nOnly English letters, numbers and underscore are allowed in table name.\n\
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

# CONFIG TESTS
def test_read_config(config_path):
    """
    Test config path.
    """
    test_var_type(config_path, "config_path", str)
    test_var_len_more_than(config_path, "config_path", 0)
    test_is_file_exists(config_path)
    assert config_path.endswith(".yaml"),\
        "\n\nExpected `.yaml` format."
    return ()

# AREAS TESTS
def test_clean_area_children(found, found_ids):
    """
    Combine all tests for `clean_area_children`.
    """
    test_var_type(found, "found", set)
    test_var_type(found_ids, "found_ids", set)

    for area in found:
        test_var_type(area, "area", tuple)

    for area_id in found_ids:
        test_var_type(area_id, "area_id", int)
    return ()

def test_area_names(names):
    """
    Valid name must follow all these rules:
    - can contain numbers
    - can contain characters: - ́ ’ , ( ) . and whitespaces.
    - 1 <= length <= 100 characters
    """
    test_var_type(names, "names", list)
    test_var_len_more_than(names, "names", 0)
    regex = re.compile(r"[0-9a-zA-ZёЁа-яА-Я-́’,()\s\.]")
    for name in names:
        test_var_type(name, "name", str)
        test_var_len_more_than(name, "name", 0)

        assert len(name) <= 100, \
        "\n\nExpected len(name) <= 100\n\
        Got len(name) == %d" % len(name)

        forbidden_characters = regex.sub("", name)
        assert forbidden_characters == "", \
"\n\nForbidden characters `%s` in `name`.\n\n\
Valid name must follow all these rules:\n\
- can contain numbers\n\
- can contain characters: - ́ ’ , ( ) . and whitespaces." % forbidden_characters
    return ()

# VACANCIES TESTS
def test_load_vacancies(headers, filters):
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

# TELEGRAM TESTS
def test_filter_vacancies(msg_columns):
    """
    Tests for `filter_vacancies`.
    """
    test_var_type(msg_columns, "msg_columns", list)
    test_var_len_more_than(msg_columns, "msg_columns", 0)
    for column in msg_columns:
        test_var_type(column, "column", str)
        test_var_len_more_than(column, "column", 0)
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
        test_var_type(value, "value", (list))
    return ()
