#!/usr/bin/env python3

"""
Send got vacancies to Telegram.

Telegram bot API:
https://core.telegram.org/bots
https://core.telegram.org/bots/api
"""

import re
import sqlite3

import requests

from common_functions import DATABASE
from get_areas import AREAS_TABLE
from get_vacancies import VACANCIES_TABLE
from secret import token

INCOME_TAX = 0.13

send_columns = [
    f"{VACANCIES_TABLE}.name",
    f"{VACANCIES_TABLE}.salary_from",
    f"{VACANCIES_TABLE}.salary_to",
    f"{VACANCIES_TABLE}.salary_currency",
    f"{VACANCIES_TABLE}.salary_gross",
    f"{VACANCIES_TABLE}.address_raw",
    f"{VACANCIES_TABLE}.snippet_requirement",
    f"{VACANCIES_TABLE}.snippet_responsibility",
    f"{VACANCIES_TABLE}.schedule_name",
    f"{VACANCIES_TABLE}.working_time_intervals_name",
    f"{VACANCIES_TABLE}.working_time_modes_name",
    f"{VACANCIES_TABLE}.alternate_url",
    f"{AREAS_TABLE}.name"
]


filter_ = {f"{VACANCIES_TABLE}.name": "%продаж%"}



def filter_vacancies_by_name(
        database, vacancies_table, areas_table, send_columns, filter_):
    """
    Select vacancies which DOESN'T contain any pattern in `filter_`.
    """
    print ("Filtering vacancies...")
    # in_tests.test_filter_vacancies_by_name(database, table, names)

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    columns = ", ".join(send_columns)
    query_values = f"{'?, ' * len(filter_)}"[:-2]
    print (f"columns = {columns}")
    query = f"SELECT {columns} FROM {vacancies_table} LEFT JOIN {areas_table} on\
    {vacancies_table}.area_id = {areas_table}.id\
    WHERE {vacancies_table}.name NOT LIKE ({query_values});"
    cursor.execute(query, list(filter_.values()))
    filtered_vacancies = cursor.fetchall()
    # out_tests.test_
    return (filtered_vacancies)

filtered_vacancies = filter_vacancies_by_name(
    DATABASE, VACANCIES_TABLE, AREAS_TABLE, send_columns, filter_)

def capitalize(data):
    """
    Capitalize first letter at 0 position and after point.
    """
    if isinstance(data, str):
        m = re.findall(r".*?[\.\?!]\s*", data)
        if m == []:
            data = data.capitalize()
            print (data)
            return (data)
        else:
            data = ""
            for n in m:
                data += n.capitalize()
            print (data)
            input ("enter")
            return (data)

msg = ""
for filtered_vacancy in filtered_vacancies:
    print (filtered_vacancy)
    print ()

    print (f"len(filtered_vacancies) = {len(filtered_vacancies)}")
    print ()
    title = capitalize(filtered_vacancy[0])
    salary_from = capitalize(int(filtered_vacancy[1]))
    salary_to = capitalize(int(filtered_vacancy[2]))
    salary_currency = capitalize(filtered_vacancy[3].upper())
    is_before_tax = capitalize(filtered_vacancy[4])
    address = capitalize(filtered_vacancy[5])
    requirement = capitalize(filtered_vacancy[6])
    responsibility = capitalize(filtered_vacancy[7])
    schedule_name = capitalize(filtered_vacancy[8])
    working_time_intervals = capitalize(filtered_vacancy[9])
    working_time_modes = capitalize(filtered_vacancy[10])
    alternate_url = filtered_vacancy[11]
    city = capitalize(filtered_vacancy[12])

    if salary_currency == "Rur":
        salary_currency = "&#x20bd;"
    if is_before_tax:
        salary_from -= salary_from * INCOME_TAX
        salary_to -= salary_to * INCOME_TAX
        salary_from = int(salary_from)
        salary_to = int(salary_to)

    msg = f"<a href='{alternate_url}'>{title}</a>\n\
<em>{salary_from}-{salary_to} {salary_currency}</em>\n\n\
{responsibility}\n\n\
{requirement}\n\n\
{schedule_name}, {working_time_intervals}, {working_time_modes}\n\n\
{address}"

    message_params = {
        "chat_id": 383837232,
        "text": msg,
        "parse_mode": "HTML"
    }

    response = requests.get(
    f"https://api.telegram.org/bot{t_value}/sendMessage", params=message_params)
    print (f"response_text = {response.text}")

    print ()
    print (f"len(filtered_vacancies) = {len(filtered_vacancies)}")
    input ("enter")

# response = requests.get(
#     f"https://api.telegram.org/bot{t_value}/getUpdates")
# response = requests.get(
