#!/usr/bin/env python3.6

"""
__main __ function for hh_parser.py
"""

from hh import *

def main():
    areas = get_areas(HEADERS, tests)
    # write_to_file('areas.json', areas, tests)
    print ()
    print ('Done!')    

if __name__ == "__main__":
    main()
