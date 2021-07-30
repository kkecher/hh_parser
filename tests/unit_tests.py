#!/usr/bin/env python3

"""
unittests for hh_parser
"""

import unittest
import get_areas as ga

# General tests


# `get_areas` tests
class TestSelectAreasByName (unittest.TestCase):
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
        not_found_names, found_names, found_ids =\
            ga.select_areas_by_name(database, areas_table, names)
        self.assertEqual(
            (len(not_found_names), len(found_names), len(found_ids)),\
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
            self.assertRaises(AssertionError, ga.select_areas_by_name,\
                              database, areas_table, [invalid_name])

if __name__ == '__main__':
    unittest.main()


# `get_vacancies` tests
