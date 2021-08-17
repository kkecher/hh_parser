#!/usr/bin/env python3

"""
Read and write to config.yaml
"""

from ruamel.yaml import YAML
import ruamel.yaml

from shared import get_table_columns_names
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def read_config(config_path="config.yaml"):
    """
    Well... read config file, default path == `./config.yaml`.
    """
    in_tests.test_read_config(config_path)

    with open (config_path, "r") as f:
        yaml = YAML()
        config = yaml.load(f)
    out_tests.test_config(config)
    return (config)

def write_config(mod_config, config_path="config.yaml"):
    """
    Well... write to config file, default path == `./config.yaml`.
    """
    with open (config_path, "w") as f:
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.dump(mod_config, f)
    out_tests.test_write_config(mod_config)
    return ()

def update_filters_columns(config, config_path="config.yaml"):
    """
    Get all  tables columns and write them to config as available to filters.
    """
    database = config["database"]
    in_tests.test_database_name(database)
    print (f"Updating {config_path} > filters_columns")
    columns = {}
    for table in config["tables"].values():
        columns[table] = get_table_columns_names(database, table)
    config["filters_columns"] = columns
    write_config(config)
    return (columns)
