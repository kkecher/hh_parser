#!/usr/bin/env python3

"""
Test functions for hh_parser.py
"""

import json
import requests
import datetime

def test_write_to_file(file_name, current_time):
      with open(file_name) as f:
          file_time = f.readline().strip()
      assert file_time == current_time,\
            'Expected record timestamp == %s.\n\
            Got file timestamp == %s' % (current_time, file_time)
      return ()

def test_get_areas_status_code(response, areas):
      """
      Test if we got status code 200.
      """
      assert response.status_code == 200,\
            'Expected status code == 200.\n\
            Got status code == %s' % response.status_code
      return ()

def test_get_areas_root_areas_quantity(response, areas):
      """
      Today (2021-07-17) HH returns 9 areas at json root:
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
            'Expected root areas number >= 9.\n\
            Got len(json) == %d' % len(areas)
      return ()

def test_get_areas_root_dict_keys(response, areas):
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
      test_get_areas_status_code(response, areas)
      test_get_areas_root_areas_quantity(response, areas)
      test_get_areas_root_dict_keys(response, areas)

def test_filter_areas(FILTERS_areas, russian_regions, filtered_areas_indexes, filtered_areas_ids):
      """
      Test 'filter_areas' function.
      """
      FILTERS = {}
      FILTERS['area'] = FILTERS_areas
      for region_index, region_id in enumerate(filtered_areas_ids):
            area_index = filtered_areas_indexes[region_index]
            assert russian_regions[area_index]['name'] in FILTERS['area'] and russian_regions[area_index]['id'] == region_id, "Areas filter error. Here is what we know:\n FILTERS['area']: %s\nfiltered_areas_ids: %s" % (FILTERS['area'], filtered_areas_ids)

def test_get_vacancies_0(FILTERS, r, vacancies):
    """
    Test responce and JSON structure got in 'get_vacancies' function.
    """
    assert r.status_code == 200, 'Expected status code == 200.\nGot status code == %s' % r.status_code
    print ()

    #Check if all expected fields is in responce[0].
    check_fields = ["items", "found",   "pages",  "per_page",  "page", "clusters",  "arguments",  "alternate_url"]
    for check_field in check_fields:
          assert check_field in vacancies, 'Expected "%s" to be in JSON[0] but has got no one.' % check_field
    return True

def test_get_vacancies_1(HEADERS, FILTERS, vacancy):
      """
      Test if vacancies attributes got in 'get_vacancies' function meet  FILTERS requirements.
      """
      #Check if only data with valid filters is in response.
      vacancy_area_url = vacancy['area']['url']
      area_properties = requests.get(vacancy_area_url, headers=HEADERS).json()
      area_id, area_parent_id = int(area_properties['id']), int(area_properties['parent_id'])
      assert area_id in FILTERS['area'] or area_parent_id in FILTERS['area'], "Some areas is here with no respect to FILTERS['area'].\nAt least this guy: %s" % vacancy

      #Transform '2021-03-22T11:25:05+0300' published date format to '2021-03-22'
      #And convert to date object
      published_date = vacancy['published_at'].split('T')[0]
      published_date = datetime.datetime.strptime(published_date, '%Y-%m-%d').date()

      filter_date_from = FILTERS['date_from']
      filter_date_from = datetime.datetime.strptime(filter_date_from, '%Y-%m-%d').date()
      assert published_date >= filter_date_from, "Some vacancies were created than FILTERS require.\nAt least this guy: %s" % vacancy
      return True
