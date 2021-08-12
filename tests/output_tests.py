#!/usr/bin/env python3

"""
Output tests for hh_parser.
"""

import json
import os.path
import sqlite3

import requests

from tests.input_tests import (
      test_var_type,
      test_var_len_more_than,
      test_table_name
)

# COMMON TESTS
def test_is_status_code_200(response):
      """
      Test if we got status code 200.
      """
      assert response.status_code == 200, \
            "Expected status code == 200.\n\
            Got status code == %s" % response.status_code
      return ()

def test_is_file_exists(file_name):
      """
      Test if data was written to file.
      """
      assert os.path.isfile(file_name), \
            "file `%s` was not created." % file_name
      return (True)

def test_create_table_columns(
            database, table, get_table_columns_names, user_columns):
      """
      Test if database table columns was created.
      """
      user_columns_names = [user_column.split()[0] \
                            for user_column in user_columns \
                            if user_column.split()[0] not in ["PRIMARY", "FOREIGN"]]
      database_columns_names = get_table_columns_names(
            database,  table)
      for user_column_name in user_columns_names:
            assert user_column_name in database_columns_names, \
            "Table or columns was not created in database.\n\
            user_columns_names = %s\n\
            database_columns_names = %s" \
            % (user_columns_names, database_columns_names)
      return ()

def test_get_table_columns_names(columns_names):
      """
      Test returned list of columns names.
      """
      test_var_type(columns_names, "columns_names", list)
      test_var_len_more_than(columns_names, "columns_names", 0)

      for column in columns_names:
            test_var_type(column, "column", str)
            test_var_len_more_than(column, "column", 0)
      return ()

def test_write_to_database(database_changes_number, counter):
      """
      Check if all data were written to database.
      """
      assert database_changes_number == counter, \
      "Expected %s database changes.\n\
      Got %s database changes." % (counter, database_changes_number)
      return ()

# def test_rename_json_to_database_key(key):
#     """
#     """
#     test_var_type(key, "key", str)
#     test_var_len_more_than(key, "key", 0)
#     test_table_name(key)
#     return ()

# AREAS TESTS
def test_load_areas_root_length(areas):
      """
      Today (2021-07-17) hh returns 9 areas at json root:
      1. Россия
      2. Украина
      3. Казахстан
      4. Азербайджан
      5. Беларусь
      6. Грузия
      7. Другие регионы
      8. Кыргызстан
      9. Узбекистан
      """
      test_var_len_more_than(areas, "areas", 8)
      return ()

def test_load_areas_root_keys(areas):
      """
      Check if root dicts has all keys:
      - id
      - parent_id
      - name
      - areas
      """
      assert list(areas[0].keys()) == ["id", "parent_id", "name", "areas"], \
            "Expected top json keys: ['id', 'parrent_id', 'name', 'areas']\n\
            Got top json keys: %s" % list(areas[0].keys())
      return ()

def test_get_user_inputs(user_areas):
      """
      """
      test_var_type(user_areas, "user_areas", list)

      for user_area in user_areas:
            test_var_type(user_area, "user_area", str)            
            test_var_len_more_than(user_area, "user_area", 0)      
      return ()

def test_load_areas(response, areas):
      """
      Combine all load_areas tests.
      """
      test_is_status_code_200(response)
      test_load_areas_root_length(areas)
      test_load_areas_root_keys(areas)
      return ()

def test_select_areas_by_name(not_found, found, found_ids):
      """
      """
      test_var_type(not_found, "not_found", set)
      test_var_type(found, "found", set)
      test_var_type(found_ids, "found_ids", set)

      for name in not_found:
            test_var_type(name, "name", str)

      for name in found:
            test_var_type(name, "name", tuple)

      for id_ in found_ids:
            test_var_type(id_, "id", int)
      return ()

def test_clean_area_children(found, cleaned, duplicated):
      """
      Test if `cleaned` is set and
      len(cleaned) + len(duplicated) == len(found)
      """
      test_var_type(cleaned, "cleaned", set)

      assert len(cleaned) + len(duplicated) == len(found), \
            "`len(cleaned) + len(duplicated)` \
            must  be even to `len(found)`.\n\
            len(cleaned) + len(duplicated) == %d, len(found) = \
            %d" % (len(cleaned) + len(duplicated_names), len(found))
      return ()


# VACANCIES TESTS
def test_load_vacancies__root_length(vacancies):
      """
      Today (2021-07-22) hh returns 8 items at json root:
      1. "items"
      2. "found"
      3. "pages"
      4. "per_page"
      5. "page"
      6. "clusters"
      7. "arguments"
      8. "alternate_url"
      """
      test_var_len_more_than(vacancies, "vacancies", 7)
      return ()

def test_load_vacancies__root_keys(vacancies):
      """
      Check if root dicts has all keys:
      - items
      - found
      - pages
      - per_page
      - page
      - clusters
      - arguments
      - alternate_url
      """
      assert list(vacancies.keys()) == ["items", "found", "pages", "per_page", "page", "clusters", "arguments", "alternate_url"], \
            "Expected top json keys: ['items', 'found', 'pages', 'per_page', 'page', 'clusters', 'arguments', 'alternate_url']\n\
            Got top json keys: %s" % list(vacancies.keys())
      return ()

def test_load_vacancies__is_respect_filters(filters, vacancies):
      """
      Test if recieved vacancies respect filters.
      """
      hh_alternate_url = vacancies["alternate_url"]
      for key, value in filters.items():
            assert key in hh_alternate_url, \
                  "`%s` param NOT in hh_alternate_url:\n\
                  %s" % (key, hh_alternate_url)
            if isinstance(value, list):
                  for param in value:
                          assert str(param) in hh_alternate_url, \
                          "`%s` param NOT in hh_alternate_url:\n\
                          %s" % (str(param), hh_alternate_url)
            else:
                  assert str(value) in hh_alternate_url, \
                  "`%s` param NOT in hh_alternate_url:\n\
                  %s" % (str(value), hh_alternate_url)
      return ()

def test_load_vacancies(response, vacancies, filters):
      """
      Combine all `get_vacancies` tests.
      """
      test_is_status_code_200(response)
      test_load_vacancies__root_length(vacancies)
      test_load_vacancies__root_keys(vacancies)
      test_load_vacancies__is_respect_filters(filters, vacancies)
      return ()


# TELEGRAM TESTS
def test_format_filters_to_query(filters_query_part, inverse_filters_query_part, filters):
      """
      Test if `filters_query_part` is correct sql query part.
      """
      test_var_type(filters_query_part, "filters_query_part", str)
      test_var_len_more_than(filters_query_part, "filters_query_part", 15)
      test_var_type(inverse_filters_query_part, "inverse_filters_query_part", str)
      test_var_len_more_than(inverse_filters_query_part, "inverse_filters_query_part", 11)

      and_substring_count = filters_query_part.count(" AND ")
      assert (len(filters) - 1) == and_substring_count, \
      "Expected %d ` AND ` subsring in `filters_query_part`\n\
      Got %d ` AND ` ones." % (len(filters)-1, and_substring_count)

      assert filters_query_part[-2:] == "?)", \
      "Expected `?)` at the end of `filters_query_part`\n\
      Got %s" % filters_query_part[-2:]

      or_substring_count = inverse_filters_query_part.count(" OR ")
      assert (len(filters) - 1) == or_substring_count, \
      "Expected %d ` OR ` subsring in `inverse_filters_query_part`\n\
      Got %d ` OR ` ones." % (len(filters)-1, and_substring_count)

      assert inverse_filters_query_part[-2:] == "?)", \
      "Expected `?)` at the end of `inverse_filters_query_part`\n\
      Got %s" % inverse_filters_query_part[-2:]
      return ()

def test_filter_vacancies(filtered_vacancies, send_columns):
      """
      Tests for `filtered_vacancies`.
      """
      test_var_type(filtered_vacancies, "filtered_vacancies", list)

      for filtered_vacancy in filtered_vacancies:
            test_var_type(filtered_vacancy, "filtered_vacancy", dict)
            test_var_len_more_than(filtered_vacancy, "filtered_vacancy", len(send_columns)-1)
            for key, value in filtered_vacancy.items():
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
    test_var_type(data, "data", (str, int, float))
    return ()
