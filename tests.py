#!/usr/bin/env python3

"""
Test functions for hh_parser.py
"""

import json
import requests

def test_get_areas(r, areas):
      """
      Test get_areas function.
      """
      assert r.status_code == 200, 'Expected status code == 200.\nGot status code == %s' % r.status_code
      assert len(areas) >= 9, 'Expected len(json) >= 9.\n Got len(json) == %d' % len(areas)
      assert list(areas[0].keys()) == ['id', 'parent_id', 'name', 'areas'], "Expected top json keys: ['id', 'parrent_id', 'name', 'areas']\nGot top json keys: %s" % list(areas[0].keys())
      return ()

def test_filter_areas(FILTERS_areas, russian_regions, filtered_areas_indexes, filtered_areas_ids):
      """
      Test filter_areas function.
      """
      FILTERS = {}
      FILTERS['areas'] = FILTERS_areas
      for region_index, region_id in enumerate(filtered_areas_ids):
            area_index = filtered_areas_indexes[region_index]
            assert russian_regions[area_index]['name'] in FILTERS['areas'] and russian_regions[area_index]['id'] == region_id, "Areas filter error. Here is what we know:\n FILTERS['areas']: %s\nfiltered_areas_ids: %s" % (FILTERS['areas'], filtered_areas_ids)

def test_get_vacancies(filtered_areas_ids, r, vacancies, HEADERS):
    """
    Test get_vacancies function.
    """
    assert r.status_code == 200, 'Expected status code == 200.\nGot status code == %s' % r.status_code

    #Check if all expected fields is in responce[0].
    check_fields = ["items", "found",   "pages",  "per_page",  "page", "clusters",  "arguments",  "alternate_url"]
    for check_field in check_fields:
      assert check_field in vacancies, 'Expected "%s" to be in JSON[0] but has got no one.' % check_field

      #Check if only data with valid areas is in response.
      for vacancy in vacancies['items']:
            vacancy_area_url = vacancy['area']['url']
            area_properties = requests.get(vacancy_area_url, headers=HEADERS)
            area_properties = area_properties.json()
            area_id = area_properties['id']
            area_parent_id = area_properties['parent_id']
            assert area_id in filtered_areas_ids or area_parent_id in filtered_areas_ids, "Some areas is here with no respect to FILTERS['areas'].\nAt least this guy: %s" % vacancy
      return True

def test_write_to_file(file_, write_to_file_time):
    with open(file_) as f:
          file_time = f.readline().strip()
          assert file_time == write_to_file_time, 'Expected write time == %s.\nGot: %s' % (write_to_file_time, file_time)
