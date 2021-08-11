#!/usr/bin/env python3.6

"""
Get vacancies from db, filter them, format and send to Telegram.

Telegram bot API:
https://core.telegram.org/bots
https://core.telegram.org/bots/api
"""

import datetime
import os
from pathlib import Path
import re
import sqlite3

import requests

from common_functions import (
        read_config
)
import tests.input_tests as in_tests
import tests.output_tests as out_tests

config = read_config()
database = config["database"]
areas_table = config["areas_table"]
vacancies_table = config["vacancies_table"]
income_tax = config["income_tax"]
chat_id = config["chat_id"]
token = os.environ["hh_bot_token"]

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
filters = {
    f"{vacancies_table}.name": "продаж|продавец",
    f"{vacancies_table}.snippet_responsibility": "продавец",
    f"{vacancies_table}.snippet_requirement": "продавец"
}

def format_filters_to_query(filters):
    """
    Format `filters` to sql query part.
    Also create inverse filters query to check filtered vacancies.
    """
    in_tests.test_format_filters_to_query(filters)

    filters_query_part = ""
    inverse_filters_query_part = ""
    for key, value in filters.items():
        filters_query_part += f"{key} NOT REGEXP (?) AND "
        inverse_filters_query_part += f"{key} REGEXP (?) OR "
    filters_query_part = filters_query_part[:-len(" AND ")]
    inverse_filters_query_part = inverse_filters_query_part[:-len(" OR ")]
    out_tests.test_format_filters_to_query(
        filters_query_part, inverse_filters_query_part, filters)
    return (filters_query_part, inverse_filters_query_part)

def regexp(expr, item):
    if item is None:
        return False
    else:
        reg = re.compile(expr)
        return reg.search(item) is not None

def filter_vacancies(
        database, vacancies_table, areas_table, send_columns, filters, \
        filters_query_part, inverse_filters_query_part):
    """
    Select vacancies which contain and don't contain patterns from `filters` and
    return them as list of tuples.
    """
    print ("Filtering vacancies...")
    in_tests.test_filter_vacancies(
        database, vacancies_table, areas_table, send_columns, filters, \
        filters_query_part, inverse_filters_query_part)

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
        filters_query, list(filters.values()))
    clean_vacancies = [dict(zip(send_columns, clean_value)) for
                       clean_value in clean_values]
    dirty_values = cursor.execute(
        inverse_filters_query, list(filters.values()))
    dirty_vacancies = [dict(zip(send_columns, dirty_value)) for
                       dirty_value in dirty_values]
    cursor.close()
    connection.close()
    out_tests.test_filter_vacancies(clean_vacancies, send_columns)
    out_tests.test_filter_vacancies(dirty_vacancies, send_columns)
    return (clean_vacancies, dirty_vacancies)

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

def write_filtered_vacancies_to_file(filters, clean_vacancies, dirty_vacancies):
    """
    Format and write filtered vacancies to file.
    File name == file creation timestamp.
    This function is for debugging purpose only.
    """
    in_tests.test_write_filtered_vacancies_to_file(
        filters, clean_vacancies, dirty_vacancies)

    Path("./data/clean_vacancies/").mkdir(parents=True, exist_ok=True)
    Path("./data/dirty_vacancies/").mkdir(parents=True, exist_ok=True)
    timestamp = str(datetime.datetime.now())
    timestamp = replace_specials_to_underscore(timestamp)
    clean_file_name = f"./data/clean_vacancies/{timestamp}.txt"
    dirty_file_name = f"./data/dirty_vacancies/{timestamp}.txt"

    vacancies_counter = 1
    with open(clean_file_name, "w") as f:
        f.write(f"FILTERS: {filters}\n\n")
        for clean_vacancy in clean_vacancies:
            f.write(f"#{vacancies_counter}\n")
            for key, value in clean_vacancy.items():
                f.write (f"{key}: {value}\n")
            f.write(f"\n\n")
            vacancies_counter += 1
        f.write(f"FILTERS: {filters}\n\n")

    vacancies_counter = 1
    with open(dirty_file_name, "w") as f:
        f.write(f"FILTERS: {filters}\n\n")
        for dirty_vacancy in dirty_vacancies:
            f.write(f"#{vacancies_counter}\n")
            for key, value in dirty_vacancy.items():
                f.write (f"{key}: {value}\n")
            f.write(f"\n\n")
            vacancies_counter += 1
        f.write(f"FILTERS: {filters}\n\n")

    out_tests.test_is_file_exists(clean_file_name)
    out_tests.test_is_file_exists(dirty_file_name)
    return ()

def format_values(data):
    """
    1. Capitalize at 0 position and after [.?!] characters
    2. Replace None with ""
    type(data) == [str, int, float, None]
    """
    in_tests.test_format_values(data)

    if isinstance(data, str):
        print (data)
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
    print (formated_data)
    out_tests.test_format_values(formated_data)
    return (formated_data)

def send_to_telegram(vacancies, chat_id, token):
    """
    Format filtered vacancies and send to Telegram.
    """
    in_tests.test_send_to_telegram(vacancies, chat_id, token)
    print ("Sending to Telegram...")

    for vacancy in vacancies:
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
<em>{salary_from}-{salary_to} {salary_currency}</em>\n\n\
{responsibility}\n\n\
{requirement}\n\n\
{schedule}, {working_time_intervals}, {working_time_modes}\n\n\
{city}, {address}\n\n\
<em>Добавлено: {created}</em>"

        msg = msg.replace(", ,", ", ")
        msg = msg.replace("  ", " ")
        msg = msg.replace(",\n", "\n")
        msg = msg.replace(", \n", "\n")

        msg_params = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML"
        }
        response = requests.get(
        f"https://api.telegram.org/bot{token}/sendMessage", params=msg_params)
        out_tests.test_is_status_code_200(response)
    return ()

def main():
    filters_query_part, inverse_filters_query_part = \
        format_filters_to_query(filters)
    clean_vacancies, dirty_vacancies = filter_vacancies(
        database, vacancies_table, areas_table, send_columns, filters, \
        filters_query_part, inverse_filters_query_part)
    write_filtered_vacancies_to_file(filters, clean_vacancies, dirty_vacancies)
    send_to_telegram(clean_vacancies, chat_id, token)

    print ()
    print ("`send_to_telegram` done!")

if __name__ == "__main__":
    main()
