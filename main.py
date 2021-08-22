#!/usr/bin/env python3.6

"""
Main module for hh_parser project.
"""

from copy import deepcopy

from areas import get_areas, search_user_areas
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
    database = deepcopy(config["database"])
    areas_table = deepcopy(config["tables"]["areas_table"])
    try:
        is_table_exists(database, areas_table)
    except AssertionError:
        get_areas(config)
    config_with_user_areas = search_user_areas(config)
    write_config(config_with_user_areas)
    get_vacancies(config_with_user_areas)
    send_to_telegram(config_with_user_areas)

    print ("\n\nAll tasks done!")

if __name__ == "__main__":
    main()
