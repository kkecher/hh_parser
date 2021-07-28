#!/usr/bin/env python3

"""
Output tests for hh_parser.py
"""

import json
import os.path
import sqlite3

import requests

from tests.input_tests import test_var_type, test_var_len_more_than

def test_is_status_code_200(response):
      """
      Test if we got status code 200.
      """
      assert response.status_code == 200,\
            "Expected status code == 200.\n\
            Got status code == %s" % response.status_code
      return ()

def test_get_areas_root_length(areas):
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

def test_get_areas_root_keys(areas):
      """
      Check if root dicts has all keys:
      - id
      - parent_id
      - name
      - areas
      """
      assert list(areas[0].keys()) == ["id", "parent_id", "name", "areas"],\
            "Expected top json keys: ['id', 'parrent_id', 'name', 'areas']\n\
            Got top json keys: %s" % list(areas[0].keys())
      return ()

def test_get_areas(response, areas):
      """
      Combine all area tests.
      """
      test_is_status_code_200(response)
      test_get_areas_root_length(areas)
      test_get_areas_root_keys(areas)
      return ()

def test_write_to_file(file_name):
      """
      Test if data was written to file.
      """
      assert os.path.isfile(file_name),\
            "file `%s` was not created." % file_name
      return ()

def test_create_table_columns(
            database, table, get_table_columns_names, user_columns):
      """
      Test if database table columns was created.
      """
      user_columns_names = [user_column.split()[0]\
                            for user_column in user_columns]
      database_columns_names = get_table_columns_names(
            database,  table)
      for user_column_name in user_columns_names:
            assert user_column_name in database_columns_names,\
            "Table or columns was not created in database.\n\
            user_columns_names = %s\n\
            database_columns_names = %s"\
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

def test_insert_into_table(database_changes_count):
      """
      Check if database_changes_count == 1.
      """
      assert database_changes_count == 1,\
      "Expected `database_changes_count` == 1\n\
      Got `database_changes_count` == %d" % database_changes_count
      return ()

def test_write_areas_to_database(database_changes_number, areas_counter):
      """
      Check if all data were written to database.
      """
      assert database_changes_number == areas_counter,\
      "Expected %s database changes.\n\
      Got %s database changes." % (areas_counter, database_changes_number)
      return ()

def test_select_areas_by_name(not_found_names, found_names, found_ids):
      """
      Tests for `select_areas_by_name` output.
      """
      test_var_type(not_found_names, "not_found_names", set)
      test_var_type(found_names, "found_names", set)
      test_var_type(found_ids, "found_ids", set)

      for area_name in not_found_names:
            test_var_type(area_name, "area_name", str)

      for area_name in found_names:
            test_var_type(area_name, "area_name", tuple)

      for area_name in found_ids:
            test_var_type(area_name, "area_name", int)
      return ()

def test_clean_area_duplicates(found_names, cleaned_names):
      """
      Test if `cleaned_names` is `set` and
      len(cleaned_names) <= len(found_names)
      """
      assert isinstance(cleaned_names, set),\
            "`cleaned_names` must be `set`.\n\
            `cleaned_names` is %s" % str(type(cleaned_names))

      assert len(cleaned_names) <= len(found_names),\
            "`len(cleaned_names` must be less or even to `len(found_names)`.\n\
            len(cleaned_names) = %d, len(found_names) = %s" %\
            (len(cleaned_names), len(found_names))
      return ()

def test_get_vacancies_root_length(response, vacancies):
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
      assert len(vacancies) >= 8,\
            'Expected root length >= 8.\n\
            Got len(json) == %d' % len(vacancies)
      return ()

def test_get_vacancies_is_respect_filters(filters, vacancies):
      """
      Test if recieved vacancies respect filters.
      """
      hh_alternate_url = vacancies["alternate_url"]
      for key, value in filters.items():
            assert key in hh_alternate_url,\
                  '`%s` param NOT in hh_alternate_url:\n\
                  %s' % (key, hh_alternate_url)
            if isinstance(value, (list, tuple)):
                  for param in value:
                          assert str(param) in hh_alternate_url,\
                          '`%s` param NOT in hh_alternate_url:\n\
                          %s' % (str(param), hh_alternate_url)
            else:
                  assert str(value) in hh_alternate_url,\
                  '`%s` param NOT in hh_alternate_url:\n\
                  %s' % (value, hh_alternate_url)
      return ()

def test_get_vacancies(response, vacancies, filters):
      test_is_status_code_200(response)
      test_get_vacancies_root_length(response, vacancies)
      test_get_vacancies_is_respect_filters(filters, vacancies)
      return ()
