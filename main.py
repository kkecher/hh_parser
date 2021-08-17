#!/usr/bin/env python3

"""
main module for hh_parser project.
"""

from areas import clean_area_children, get_areas, search_user_areas
from config import read_config, write_config
from shared import is_table_exists
from telegram import send_to_telegram
from vacancies import get_vacancies

def main():
    """
    Get vacancies in user specified regions, filter them
    and send to Telegram.
    """
    config = read_config()
    database = config["database"]
    areas_table = config["tables"]["areas_table"]
    try:
        is_table_exists(database, areas_table)
    except AssertionError:
        get_areas(config)
    found_areas_ids = search_user_areas(database, areas_table)
    print (found_areas_ids)
    config["url_filters"]["area"] = found_areas_ids
    config["database_filters"]["{areas_table}.id"] = ["REGEXP", "|".join(
        str(found_area_id) for found_area_id in found_areas_ids)]
    write_config(config)
    get_vacancies(config)
    input ("main enter")
    send_to_telegram(config, found_areas_ids)

    print ("\n\nAll tasks done!")

if __name__ == "__main__":
    main()
