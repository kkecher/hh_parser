#!/usr/bin/env python3

"""
unittests for hh_parser.
"""

from config import read_config
import areas
import telegram
import unittest

# SHARED TESTS


# AREAS TESTS
class TestSelectAreasByName(unittest.TestCase):
    def test_select_areas_by_name(self):
        config = read_config()
        database = config["database"]
        areas_table = config["tables"]["areas_table"]
        names = [
        "а",
        "Я",
        "-",
        "-в",
        "с.п",
        "Москва",
        "владивосток",
        "ростов-на-Дону",
        25*"пиза",
        "благовещенск (амурская область)",
        "ёбург",
        "Ёлки-п.к",
        "1",
        "Горки-10 (Московская область, Одинцовский район)",
        "Мосолово (Рязанская область, Шиловский район)",
        "Алекса́ндров Гай",
        "Республика Кот-д’Ивуар",
        "nyc",
        "w-e"
        ]
        not_found_names, found_names, found_ids = \
            areas.select_areas_by_name(database, areas_table, names)
        self.assertEqual(
            (len(not_found_names), len(found_names), len(found_ids)), \
            (6, 3817, 3817)
        )

    def test_is_invalid_name(self):
        config = read_config()
        database = config["database"]
        areas_table = config["tables"]["areas_table"]
        invalid_names = [
            "",
            25*"пиза" + "п"
            "моск;ква"
      ]
        for invalid_name in invalid_names:
            self.assertRaises(AssertionError, areas.select_areas_by_name, \
                              database, areas_table, [invalid_name])

# VACANCIES TESTS

# TELEGRAM TESTS
class TestFormatFiltersToQuery(unittest.TestCase):
    def test_format_filters_to_query(self):
        config = read_config()
        areas_table = config["tables"]["areas_table"]
        vacancies_table = config["tables"]["vacancies_table"]

        filters = {
            "{vacancies_table}.name": ["NOT REGEXP", "прода"]
        }
        query_filters = \
            telegram.format_filters_to_query(filters, config)
        filters_query_part = query_filters[1]
        inverse_filters_query_part = query_filters[2]
        self.assertEqual(
        filters_query_part, "vacancies.name NOT REGEXP (?)")
        self.assertEqual(
        inverse_filters_query_part, "(vacancies.name REGEXP (?))")

        filters = {
            "{vacancies_table}.name": ["NOT REGEXP", "гара", "нед"]
        }
        query_filters = \
            telegram.format_filters_to_query(filters, config)
        filters_query_part = query_filters[1]
        inverse_filters_query_part = query_filters[2]
        self.assertEqual(
        filters_query_part, "vacancies.name NOT REGEXP (?)")
        self.assertEqual(
        inverse_filters_query_part, "(vacancies.name REGEXP (?))")

        filters = {
            "{vacancies_table}.is_sent": ["==", 1],
            "{areas_table}.id": ["REGEXP", 42],
            "{vacancies_table}.name": ["NOT REGEXP", "пиш"],
            "{vacancies_table}.snippet_responsibility": ["LIKE", "вд40"]
        }
        query_filters = \
            telegram.format_filters_to_query(filters, config)
        filters_query_part = query_filters[1]
        inverse_filters_query_part = query_filters[2]
        self.assertEqual(
        filters_query_part, "vacancies.is_sent == (?) AND areas.id REGEXP (?) \
AND vacancies.name NOT REGEXP (?) AND \
vacancies.snippet_responsibility LIKE (?)")
        self.assertEqual(
        inverse_filters_query_part, "vacancies.is_sent == (?) AND \
areas.id REGEXP (?) AND (vacancies.name REGEXP (?) OR \
vacancies.snippet_responsibility NOT LIKE (?))")

class TestFormatMsgValues(unittest.TestCase):
    def test_format_msg_values(self):
        in_sentence = "реализация намеченых плановых заданий требует \
от нас анализа новых предложений."
        out_sentence = telegram.format_msg_values(in_sentence)
        self.assertEqual(
            out_sentence, "Реализация намеченых плановых заданий требует \
от нас анализа новых предложений."
            )

        in_sentence = 42
        out_sentence = telegram.format_msg_values(in_sentence)
        self.assertEqual(out_sentence, 42)

        in_sentence = 3.14
        out_sentence = telegram.format_msg_values(in_sentence)
        self.assertEqual(out_sentence, 3.14)

        in_sentence = None
        out_sentence = telegram.format_msg_values(in_sentence)
        self.assertEqual(out_sentence, "")

        in_sentence = "товарищи!рамки и место обучения кадоров позволяет \
выполнить важные задания по разработке.существенных финансовых!и \
административных условий ?       с другой стороны .постоянный и \
количественный рост,сфера нашей активности?обеспечивают широкому \
кругу (специалистов!) участие в формировании ?дальнейших направлений \
развития. 9,81! 2.17? именно так."
        out_sentence = telegram.format_msg_values(in_sentence)
        self.assertEqual(
            out_sentence, "Товарищи!Рамки и место обучения кадоров позволяет \
выполнить важные задания по разработке.Существенных \
финансовых!И административных условий ?       С другой стороны \
.Постоянный и количественный рост,сфера нашей \
активности?Обеспечивают широкому кругу (специалистов!) участие в \
формировании ?Дальнейших направлений развития. 9,81! 2.17? \
Именно так."
            )

if __name__ == "__main__":
    unittest.main()
