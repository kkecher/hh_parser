#!/usr/bin/env python3.6

"""
Get vacancies from db, filter them, format and send to Telegram.

Telegram bot API:
https://core.telegram.org/bots
https://core.telegram.org/bots/api
"""

from pathlib import Path
import datetime
import os
import re
import sqlite3
import time

import requests

import tests.input_tests as in_tests
import tests.output_tests as out_tests

def format_filters_to_query(filters_not_regex):
    """
    Format filters to sql query part.
    Also create inverse filters query to check filtered vacancies.
    """
    in_tests.test_format_filters_to_query(filters_not_regex)

    filters_query_part = ""
    inverse_filters_query_part = ""
    for key, value in filters_not_regex.items():
        filters_query_part += f"{key} NOT REGEXP (?) AND "
        inverse_filters_query_part += f"{key} REGEXP (?) OR "
    filters_query_part = filters_query_part[:-len(" AND ")]
    inverse_filters_query_part = inverse_filters_query_part[:-len(" OR ")]
    filters = [
        filters_not_regex, filters_query_part, inverse_filters_query_part]
    out_tests.test_format_filters_to_query(filters)
    return (filters)

def regexp(expr, item):
    if item is None:
        return False
    else:
        reg = re.compile(expr)
        return reg.search(item) is not None

def filter_vacancies(config, send_columns, filters):
    """
    Select vacancies which contain and don't contain patterns from `filters` and
    return them as list of dicts.
    """
    database = config["database"]
    vacancies_table = config["vacancies_table"]
    areas_table = config["areas_table"]
    filters_not_regex = filters[0]
    filters_query_part = filters[1]
    inverse_filters_query_part = filters[2]
    in_tests.test_database_name(database)
    in_tests.test_filter_vacancies(send_columns, filters)
    print ("\n\nFiltering vacancies...")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    connection.create_function("REGEXP", 2, regexp)
    send_columns_query = ", ".join(send_columns)
    filters_query = f"SELECT {send_columns_query} FROM \
{vacancies_table} LEFT JOIN {areas_table} ON {vacancies_table}.area_id == \
{areas_table}.id WHERE {filters_query_part};"
    inverse_filters_query =  f"SELECT {send_columns_query} FROM \
{vacancies_table} LEFT JOIN {areas_table} ON {vacancies_table}.area_id == \
{areas_table}.id WHERE {inverse_filters_query_part};"

    clean_values = cursor.execute(
        filters_query, list(filters_not_regex.values()))
    clean_vacancies = [dict(zip(send_columns, clean_value)) for
                       clean_value in clean_values]
    dirty_values = cursor.execute(
        inverse_filters_query, list(filters_not_regex.values()))
    dirty_vacancies = [dict(zip(send_columns, dirty_value)) for
                       dirty_value in dirty_values]
    filtered_vacancies = [clean_vacancies, dirty_vacancies]
    cursor.close()
    connection.close()
    out_tests.test_filter_vacancies(filtered_vacancies, send_columns)
    return (filtered_vacancies)

def replace_specials_to_underscore(string):
    """
    Replace all none alphanumeric characters to `_`.
    """
    in_tests.test_replace_specials_to_underscore(string)

    new_string = ""
    for letter in string:
        if letter.isalnum():
            new_string += letter
        else:
            new_string += "_"
    out_tests.test_replace_specials_to_underscore(new_string)
    return (new_string)

def write_filtered_vacancies_to_file(
        vacancies, filtered_path, filters_not_regex):
    """
    Format and write filtered vacancies to file.
    File name == file creation timestamp.
    This function is for debugging purpose only.
    """
    timestamp = str(datetime.datetime.now())
    timestamp = replace_specials_to_underscore(timestamp)
    file_name = f"{filtered_path}/{timestamp}.txt".replace("//", "/")

    try:
        Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        print (f"I don't have permission to create {file_name}.\n\
Try to change {file_name} var in `config.yaml` file or just solve this.")

    vacancies_counter = 1
    with open(file_name, "w") as f:
        f.write(f"FILTERS: {filters_not_regex}\n\n")
        for vacancy in vacancies:
            f.write(f"#{vacancies_counter}\n")
            for key, value in vacancy.items():
                f.write (f"{key}: {value}\n")
            f.write(f"\n\n")
            vacancies_counter += 1
        f.write(f"FILTERS: {filters_not_regex}\n\n")

    out_tests.test_is_file_exists(file_name)
    return ()

def format_values(data):
    """
    1. Capitalize at 0 position and after [.?!] characters
    2. Replace None with ""
    type(data) == [str, int, float, None]
    """
    in_tests.test_format_values(data)

    if isinstance(data, str):
        sentences = re.findall(r".*?(?:\.|\?|!|$)\s*", data)
        if sentences == []:
            formated_data = data.capitalize()
            out_tests.test_format_values(formated_data)
            return (formated_data)
        else:
            formated_data = ""
            for sentence in sentences:
                formated_data += sentence.capitalize()
    elif isinstance(data, type(None)):
        formated_data = ""
    else:
        formated_data = data
    out_tests.test_format_values(formated_data)
    return (formated_data)

def build_msg(vacancy, config):
    """
    Build html of one vacancy.
    """
    areas_table = config["areas_table"]
    vacancies_table = config["vacancies_table"]
    income_tax = config["income_tax"]
    in_tests.test_var_type(income_tax, "income_tax", (int, float))

    title = \
format_values(vacancy[f"{vacancies_table}.name"])
    alternate_url = \
vacancy[f"{vacancies_table}.alternate_url"]
    salary_from = \
format_values(vacancy[f"{vacancies_table}.salary_from"])
    salary_to = \
format_values(vacancy[f"{vacancies_table}.salary_to"])
    salary_currency = \
format_values(vacancy[f"{vacancies_table}.salary_currency"])
    is_before_tax = \
vacancy[f"{vacancies_table}.salary_gross"]
    requirement = \
format_values(vacancy[f"{vacancies_table}.snippet_requirement"])
    responsibility = \
format_values(vacancy[f"{vacancies_table}.snippet_responsibility"])
    schedule = \
format_values(vacancy[f"{vacancies_table}.schedule_name"])
    working_time_intervals = \
format_values(vacancy[f"{vacancies_table}.working_time_intervals_name"])
    working_time_modes = \
format_values(vacancy[f"{vacancies_table}.working_time_modes_name"])
    city = \
format_values(vacancy[f"{areas_table}.name"])
    address = \
format_values(vacancy[f"{vacancies_table}.address_raw"])
    created = \
vacancy[f"{vacancies_table}.created_at"]

    if salary_from != "" and salary_to != "" and salary_from > salary_to:
        salary_from, salary_to = salary_to, salary_from
    if salary_currency == "Rur":
        salary_currency = "&#x20bd;"
    if is_before_tax and salary_from != "":
        salary_from = int(salary_from - salary_from * income_tax)
    if is_before_tax and salary_to != "":
        salary_to = int(salary_to - salary_to * income_tax)

    msg = f"<a href='{alternate_url}'>{title}</a>\n\
<em>{salary_from} - {salary_to} {salary_currency}</em>\n\n\
{responsibility}\n\n\
{requirement}\n\n\
{schedule}, {working_time_intervals}, {working_time_modes}\n\n\
{city}, {address}\n\n\
<em>Добавлено: {created}</em>"

    msg = msg.replace(", ,", ", ")
    msg = msg.replace("  ", " ")
    msg = msg.replace(",\n", "\n")
    msg = msg.replace(", \n", "\n")
    out_tests.test_var_type(msg, "message", str)
    out_tests.test_var_len_more_than(msg, "message", 149)
    return (msg)

def send_to_telegram(config):
    areas_table = config["areas_table"]
    vacancies_table = config["vacancies_table"]
    chat_id = config["chat_id"]
    clean_path = config["telegram_clean_vacancies_file_path"]
    dirty_path = config["telegram_dirty_vacancies_file_path"]
    token = os.environ["hh_bot_token"]
    in_tests.test_table_name(areas_table)
    in_tests.test_table_name(vacancies_table)
    in_tests.test_var_type(chat_id, "chat_id", int)
    in_tests.test_write_to_file_file_name(clean_path)
    in_tests.test_write_to_file_file_name(dirty_path)
    in_tests.test_var_type(token, "token", str)
    in_tests.test_var_len_more_than(token, "token", 0)

    send_columns = [
        f"{vacancies_table}.name",
        f"{vacancies_table}.alternate_url",
        f"{vacancies_table}.salary_from",
        f"{vacancies_table}.salary_to",
        f"{vacancies_table}.salary_currency",
        f"{vacancies_table}.salary_gross",
        f"{vacancies_table}.snippet_responsibility",
        f"{vacancies_table}.snippet_requirement",
        f"{vacancies_table}.schedule_name",
        f"{vacancies_table}.working_time_intervals_name",
        f"{vacancies_table}.working_time_modes_name",
        f"{areas_table}.name",
        f"{vacancies_table}.address_raw",
        f"{vacancies_table}.created_at"
    ]

    # Currently filters work only for ‘not like‘ requests, i.e.
    # they will work to get vacancies not containing some pattern but
    # they will NOT work for requests like ‘salary_from > n’.
    filters_not_regex = {
        f"{vacancies_table}.name": "продаж|продавец",
        f"{vacancies_table}.snippet_responsibility": "продавец",
        f"{vacancies_table}.snippet_requirement": "продавец"
    }

    msg_params = {
        "chat_id": chat_id,
        "parse_mode": "HTML"
        }

    filters_query = format_filters_to_query(filters_not_regex)
    clean_vacancies, dirty_vacancies = filter_vacancies(
        config, send_columns, filters_query)

    for vacancies, filtered_path in zip(
            [clean_vacancies, dirty_vacancies], [clean_path, dirty_path]):
        write_filtered_vacancies_to_file(
            vacancies, filtered_path, filters_not_regex)

    print ("\n\nSending to Telegram...")
    for clean_vacancy in clean_vacancies:
        print ("#", end='', flush=True)
        msg = build_msg(clean_vacancy, config)
        msg_params["text"] = msg
        while True:
            try:
                response = requests.get(
                    f"https://api.telegram.org/bot{token}/sendMessage", \
                    params=msg_params)
                out_tests.test_is_status_code_200(response)
                break
            except AssertionError:
                sleep_time = 120
                print (
        f"\n\nPhew, I was toooo fast. Need a rest for {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue
    print (f"\n\nSent {len(clean_vacancies)} vacancies.")
    return ()
