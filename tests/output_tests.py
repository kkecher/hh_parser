#!/usr/bin/env python3

"""
Output tests for hh_parser.
"""

from copy import deepcopy
import datetime
import json
import os.path
import sqlite3
from ruamel.yaml import YAML

import requests

from tests.input_tests import (
      test_var_type,
      test_var_len_equal,
      test_var_len_more_than,
      test_table_name,
      test_dict_data_type,
      test_is_file_exists
)

# SHARED TESTS
def test_is_status_code_200(response):
      """
      Test if we got status code 200.
      """
      assert response.status_code == 200, \
            "\n\nExpected status code == 200.\n\
            Got status code == %s" % response.status_code
      return ()

def test_create_table_columns(
            database_columns_names, user_columns):
      """
      Test if database table columns was created.
      """
      user_columns_names = [user_column.split()[0] \
            for user_column in user_columns if user_column.split()[0] not in \
                            ["PRIMARY", "FOREIGN"]]
      for user_column_name in user_columns_names:
            assert user_column_name in database_columns_names, \
            "\n\nTable or columns was not created in database.\n\
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
      "\n\nExpected %s database changes.\n\
      Got %s database changes." % (counter, database_changes_number)
      return ()

def test_write_config(edited_config, config_path="config.yaml"):
      """
      Compare new config file with the the old modified one.
      """
      with open(config_path, "r", encoding="utf8") as f:
            yaml = YAML()
            new_config = yaml.load(f)
      assert new_config == edited_config, \
      "\n\nExpected config:\n %s\
x      \n\nGot config:\n %s" % (edited_config, new_config)
      return ()

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
      "\n\nExpected top json keys: ['id', 'parrent_id', 'name', 'areas']\n\
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

      for area in found:
            test_var_type(area, "area", tuple)

      for area_id in found_ids:
            test_var_type(area_id, "area_id", int)
      return ()

def test_select_areas_by_ids(not_found, found):
      """
      """
      test_var_type(not_found, "not_found", set)
      test_var_type(found, "found", set)

      for area in found:
            test_var_type(area, "area", tuple)
      return ()

def test_clean_area_children(found, cleaned, duplicated):
      """
      Test if `cleaned` is set and
      len(cleaned) + len(duplicated) == len(found)
      """
      test_var_type(cleaned, "cleaned", set)

      assert len(cleaned) + len(duplicated) == len(found), \
            "\n\n`len(cleaned) + len(duplicated)` \
            must  be equal to `len(found)`.\n\
            len(cleaned) + len(duplicated) == %d, len(found) = \
            %d" % (len(cleaned) + len(duplicated_names), len(found))
      return ()


# VACANCIES TESTS
def test_load_vacancies_root_length(vacancies):
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

def test_load_vacancies_root_keys(vacancies):
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
      assert list(vacancies.keys()) == ["items", "found", "pages", "per_page", \
            "page", "clusters", "arguments", "alternate_url"], \
            "Expected top json keys: ['items', 'found', 'pages', 'per_page', \
            'page', 'clusters', 'arpguments', 'alternate_url']\n\
            Got top json keys: %s" % list(vacancies.keys())
      return ()

def test_load_vacancies_is_respect_filters(filters, vacancies):
      """
      Test if recieved vacancies respect filters.
      """
      match_keys = {
            "per_page": "items_on_page",
            "period": "search_period"
      }

      hh_alternate_url = vacancies["alternate_url"]
      filters = deepcopy(filters)
      try:
            filters["date_from"] = datetime.datetime.fromisoformat(
                  filters["date_from"])
            filters["date_from"] = filters["date_from"].strftime(
                  "%d.%m.%Y+%H%%3A%M%%3A%S")
      except KeyError:
            pass
      for key, value in filters.items():
            try:
                  key = match_keys[key]
            except KeyError:
                  pass
            assert key in hh_alternate_url, \
                  "\n\n`%s` param NOT in hh_alternate_url:\n\
                  %s" % (key, hh_alternate_url)
            if isinstance(value, list):
                  for param in value:
                        assert str(param) in hh_alternate_url, \
                              "\n\n`%s` param NOT in hh_alternate_url:\n\
                              %s" % (str(param), hh_alternate_url)
            else:
                  assert str(value) in hh_alternate_url, \
                  "\n\n`%s` param NOT in hh_alternate_url:\n\
                  %s" % (str(value), hh_alternate_url)
      return ()

def test_load_vacancies(response, vacancies, filters):
      """
      Combine all `get_vacancies` tests.
      """
      test_is_status_code_200(response)
      test_load_vacancies_root_length(vacancies)
      test_load_vacancies_root_keys(vacancies)
      test_load_vacancies_is_respect_filters(filters, vacancies)
      return ()


# TELEGRAM TESTS
def test_format_filters_to_query(filters, query_filters):
      """
      Test if filters sql parts are syntax correct.
      """
      test_var_type(query_filters, "query_filters", list)
      test_var_len_equal(query_filters, "query_filters", 3)

      patterns = query_filters[0]
      filters_query_part = query_filters[1]
      inverse_filters_query_part = query_filters[2]

      test_var_type(patterns, "patterns", list)
      test_var_len_more_than(patterns, "patterns", len(filters)-1)
      for pattern in patterns:
            test_var_type(pattern, "pattern", str)

      test_var_type(filters_query_part, "filters_query_part", str)
      test_var_len_more_than(filters_query_part, "filters_query_part", 15)
      and_substring_count = filters_query_part.count(" AND ")
      assert (len(filters) - 1) == and_substring_count, \
      "\n\nExpected %d ` AND ` subsring in `filters_query_part`\n\
      Got %d ` AND ` ones." % (len(filters)-1, and_substring_count)
      assert filters_query_part.endswith("?)"), \
      "Expected `?)` at the end of `filters_query_part`\n\
      Got %s" % filters_query_part[-2:]
      assert "LIKE (?," not in filters_query_part,\
'\n\nLIKE operator excepts only one pattern:\n\
    valid: "{{table_name}}.{column}": [LIKE, pattern]\n\
    NOT valid: "{{table_name}}.{column}": [LIKE, "pattern_1, pattern_2"]\n\
    NOT valid: "{{table_name}}.{column}": [LIKE, [pattern_1, pattern_2]]\n\
Use REGEXP if you need muptiple patterns:\n\
    valid: "{{table_name}}.{column}": \
[REGEXP, "pattern_1|pattern_2|pattern_3"]\n\
Try to edit `config.yaml > filters`'

      test_var_type(
            inverse_filters_query_part, "inverse_filters_query_part", str)
      test_var_len_more_than(
            inverse_filters_query_part, "inverse_filters_query_part", 11)
      or_substring_count = inverse_filters_query_part.count(" OR ")
      assert max(0, (len(filters) - 3)) == or_substring_count, \
      "Expected %d ` OR ` subsring in `inverse_filters_query_part`\n\
      Got %d ` OR ` ones." % \
      (max(0, (len(filters) - 3)), or_substring_count)
      assert inverse_filters_query_part.endswith(")"), \
      "Expected `)` at the end of `inverse_filters_query_part`\n\
      Got %s" % inverse_filters_query_part[-1:]
      return ()

def test_filter_vacancies(filtered_vacancies, send_columns):
      """
      Tests for `filtered_vacancies`.
      """
      test_var_type(filtered_vacancies, "filtered_vacancies", list)
      test_var_len_more_than(filtered_vacancies, "filtered_vacancy", 1)
      test_var_type(filtered_vacancies[0], "clean_vacancies", list)
      test_var_type(filtered_vacancies[1], "dirty_vacancies", list)

      for clean_dirty_vacancies in filtered_vacancies:
            for filtered_vacancy in clean_dirty_vacancies:
                  test_var_type(filtered_vacancy, "filtered_vacancy", dict)
                  test_var_len_more_than(filtered_vacancy, "filtered_vacancy", \
                                         len(send_columns)-1)
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

def test_format_msg_values(data):
    """
    Tests for `format_msg_values`.
    """
    test_var_type(data, "data", (str, int, float))
    return ()
