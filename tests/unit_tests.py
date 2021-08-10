#!/usr/bin/env python3

"""
unittests for hh_parser
"""
import get_areas as ga
import send_to_telegram as st
import unittest

# Common tests


# `get_areas` tests
class TestSelectAreasByName(unittest.TestCase):
    def test_select_areas_by_name(self):
        database = ga.DATABASE
        areas_table = ga.AREAS_TABLE
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
            ga.select_areas_by_name(database, areas_table, names)
        self.assertEqual(
            (len(not_found_names), len(found_names), len(found_ids)), \
            (6, 3816, 3816)
        )

    def test_is_invalid_name(self):
        database = ga.DATABASE
        areas_table = ga.AREAS_TABLE
        invalid_names = [
            "",
            25*"пиза" + "п"
            "моск;ква"
      ]
        for invalid_name in invalid_names:
            self.assertRaises(AssertionError, ga.select_areas_by_name, \
                              database, areas_table, [invalid_name])

if __name__ == "__main__":
    unittest.main()


# `get_vacancies` tests


# `send_to_telegram` tests
class TestFormatFiltersToQuery(unittest.TestCase):
    def test_format_filters_to_query(self):
        VACANCIES_TABLE = "vacancies"

        filters = {
            f"{VACANCIES_TABLE}.name": ["прода"]
        }
        filters_query_part, inverse_filters_query_part = \
            st.format_filters_to_query(filters)
        self.assertEqual(
        filters_query_part, "vacancies.name NOT REGEXP (?)")
        self.assertEqual(
        inverse_filters_query_part, "vacancies.name REGEXP (?)")

        filters = {
            f"{VACANCIES_TABLE}.name": ["гара", "нед"]
        }
        filters_query_part, inverse_filters_query_part = \
            st.format_filters_to_query(filters)
        self.assertEqual(
        filters_query_part, "vacancies.name NOT REGEXP (?, ?)")
        self.assertEqual(
        inverse_filters_query_part, "vacancies.name REGEXP (?, ?)")

        filters = {
            f"{VACANCIES_TABLE}.name": ["пиш"],
            f"{VACANCIES_TABLE}.snippet_responsibility": ["дед", "вд40"]
        }
        filters_query_part, inverse_filters_query_part = \
            st.format_filters_to_query(filters)
        self.assertEqual(
        filters_query_part, "vacancies.name NOT REGEXP (?) AND \
vacancies.snippet_responsibility NOT REGEXP (?, ?)")
        self.assertEqual(
        inverse_filters_query_part, "vacancies.name REGEXP (?) AND \
vacancies.snippet_responsibility REGEXP (?, ?)")

class TestCapitalizeFirstLetter(unittest.TestCase):
    def test_capitalize_first_letter(self):
        in_sentence = "реализация намеченых плановых заданий требует от нас анализа новых предложений."
        out_sentence = st.capitalize_first_letter(in_sentence)
        self.assertEqual(
            out_sentence, "Реализация намеченых плановых заданий требует от нас анализа новых предложений."
            )

        in_sentence = 42
        out_sentence = st.capitalize_first_letter(in_sentence)
        self.assertEqual(
            out_sentence, 42
            )

        in_sentence = 3.14
        out_sentence = st.capitalize_first_letter(in_sentence)
        self.assertEqual(
            out_sentence, 3.14
            )

        in_sentence = None
        out_sentence = st.capitalize_first_letter(in_sentence)
        self.assertEqual(
            out_sentence, None
            )

        in_sentence = "товарищи!рамки и место обучения кадоров позволяет выполнить важные задания по разработке.существенных финансовых!и административных условий ?       с другой стороны .постоянный и количественный рост,сфера нашей активности?обеспечивают широкому кругу (специалистов!) участие в формировании ?дальнейших направлений развития. 9,81! 2.17? именно так."
        out_sentence = st.capitalize_first_letter(in_sentence)
        self.assertEqual(
            out_sentence, "Товарищи!Рамки и место обучения кадоров позволяет выполнить важные задания по разработке.Существенных финансовых!И административных условий ?       С другой стороны .Постоянный и количественный рост,сфера нашей активности?Обеспечивают широкому кругу (специалистов!) участие в формировании ?Дальнейших направлений развития. 9,81! 2.17? Именно так."
            )                
        
if __name__ == "__main__":
    unittest.main()
