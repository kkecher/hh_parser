#!/usr/bin/env python3

"""
Test functions for hh_parser.py
"""

import json
import requests
import os.path
import sqlite3

def test_write_to_file(file_name):
      """
      Test if data was written to file.
      """
      assert os.path.isfile(file_name),\
            "file `%s` was not created." % file_name
      return ()

def test_is_status_code_200(response):
      """
      Test if we got status code 200.
      """
      assert response.status_code == 200,\
            'Expected status code == 200.\n\
            Got status code == %s' % response.status_code
      return ()

def test_get_areas_root_length(response, areas):
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
      assert len(areas) >= 9,\
            'Expected root length >= 9.\n\
            Got len(json) == %d' % len(areas)
      return ()

def test_get_areas_root_keys(response, areas):
      """
      Check if root dicts has all keys:
      - id
      - parent_id
      - name
      - areas
      """
      assert list(areas[0].keys()) == ['id', 'parent_id', 'name', 'areas'],\
            "Expected top json keys: ['id', 'parrent_id', 'name', 'areas']\n\
            Got top json keys: %s" % list(areas[0].keys())
      return ()

def test_get_areas(response, areas):
      """
      Combine all area tests.
      """
      test_is_status_code_200(response)
      test_get_areas_root_length(response, areas)
      test_get_areas_root_keys(response, areas)

def test_create_database(database):
      """
      Check if database file was created.
      """
      assert os.path.isfile(database),\
            "Database `%s` was not created." % database
      return ()

def test_write_to_database(database_changes_number, json_counter):
      """
      Check if all data were written to database.
      """
      assert database_changes_number == json_counter,\
      "Expected %s rows in the table.\n\
      Got %s rows." % (json_counter, table_counter)
      return ()

def test_is_valid_area_name(is_valid_area_name):
      """
      Test `is_valid_area_name` function using some specific cases.
      """
      valid_names = [
            'а',
            'Я',
            '-',
            '-в',
            'с.п',
            'Москва',
            'владивосток',
            'ростов-на-Дону',
            25*'пиза',
            'благовещенск (амурская область)',
            'ёбург',
            'Ёлки-п.к',
            '1',
            'Горки-10 (Московская область, Одинцовский район)',
            'Мосолово (Рязанская область, Шиловский район)',
            'Алекса́ндров Гай',
            'Республика Кот-д’Ивуар'
      ]
      invalid_names = [
            'nyc',
            '',
            'w-e',
            25*'пиза' + 'п',
      ]

      for valid_name in valid_names:
            assert is_valid_area_name(valid_name),\
            "Expected `True` for `%s`.\n\
            Got False." % valid_name

      for invalid_name in invalid_names:
            assert not is_valid_area_name(invalid_name),\
            "Expected `False` for `%s`.\n\
            Got True." % invalid_name
      return ()

def test_select_areas_by_name(select_areas_by_name, database):
      """
      Test `select_areas_by_name` function using some specific cases.
      """
      names = [
            'drop table areas',
            'nyc',
            'пиз',
            'влад',
            'москва'
      ]

      control_invalid_names = {
            'drop table areas',
            'nyc'
      }
      control_not_found_names = {
            'пиз',
      }
      control_found_names = {
            (22, 1948, 'владивосток'),
            (23, 1716, 'владимир'),
            (82, 1475, 'владикавказ'),
            (1716, 113, 'владимирская область'),
            (1733, 1716, 'радужный (владимирская область)'),
            (3493, 2198, 'владимирец'),
            (3604, 2123, 'владимир-волынский'),
            (5514, 1438, 'владимирская (краснодарский край)'),
            (1, 113, 'москва')
      }
      control_found_ids = {
            22,
            23,
            82,
            1716,
            1733,
            3493,
            3604,
            5514,
            1,
      }

      returned_invalid_names,returned_not_found_names,returned_found_names,\
            returned_found_ids = select_areas_by_name(database, names)

      assert len(control_invalid_names) == len(returned_invalid_names),\
            "Returned and control data are not even.\n\
            control_invalid_names == %s\n\
            returned_invalid_names == %s" %\
            (control_invalid_names, returned_invalid_names)

      assert len(control_not_found_names) == len(returned_not_found_names),\
            "Returned and control data are not even.\n\
            control_not_found_names == %s\n\
            returned_not_found_names == %s" %\
            (control_not_found_names, returned_not_found_names)

      assert len(control_found_names) == len(returned_found_names),\
            "Returned and control data are not even.\n\
            control_found_names == %s\n\
            returned_found_names == %s" %\
            (control_found_names, returned_found_names)

      assert len(control_found_ids) == len(returned_found_ids),\
            "Returned and control data are not even.\n\
            control_found_ids == %d\n\
            returned_found_ids == %d" %\
            (control_found_names, returned_found_names)
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

