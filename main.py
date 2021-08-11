#!/usr/bin/env python3

"""
main function for hh_parser project.
"""
import get_areas
import get_vacancies
import send_to_telegram

def main():
    # get_areas.main()
    get_vacancies.main()
    send_to_telegram.main()

    print ()
    print ("Done!")

if __name__ == "__main__":
    main()
