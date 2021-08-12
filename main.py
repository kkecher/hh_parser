#!/usr/bin/env python3

"""
main function for hh_parser project.
"""

from pathlib import Path
import yaml

from areas import (
    clean_area_children,
    create_areas_generator,
    get_areas,
    search_user_areas,
    load_areas,
    select_areas_by_name,
    write_areas_to_database
)
from shared import (
    is_table_exists,
    read_config
)
from vacancies import (
    get_hh_vacancies,
    load_vacancies,
    write_vacancies_to_database
)
import telegram
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def main():
    """
    """
    config = read_config()
    headers = config["headers"]
    database = config["database"]
    areas_file = config["areas_file"]
    areas_table = config["areas_table"]
    vacancies_file = config["vacancies_file"]
    vacancies_table = config["vacancies_table"]

    try:
        is_table_exists(database, areas_table)
    except AssertionError:
        get_areas()
    found_areas, found_areas_ids = search_user_areas(database, areas_table)
    cleaned_names, cleaned_ids = clean_area_children(
        found_areas, found_areas_ids)
    get_hh_vacancies(
        headers, database, vacancies_table, vacancies_file, list(cleaned_ids))
    input ("enter")
    send_to_telegram.main()

    print ()
    print ("Done!")

if __name__ == "__main__":
    main()
