 #!/usr/bin/env python3.6

"""
Read and write config.yaml
"""
from copy import deepcopy

from ruamel.yaml import YAML

from shared import get_table_columns_names
import tests.input_tests as in_tests
import tests.output_tests as out_tests

def read_config(config_path="config.yaml"):
    """
    Well... read config file.
    """
    in_tests.test_read_config(config_path)

    with open (config_path, "r", encoding="utf8") as f:
        yaml = YAML()
        config = yaml.load(f)
    out_tests.test_dict_data_type(config)
    return (config)

def write_config(edited_config, config_path="config.yaml"):
    """
    Well... write to config file.
    """
    in_tests.test_read_config(config_path)
    in_tests.test_dict_data_type(edited_config)

    with open (config_path, "w", encoding="utf8") as f:
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.dump(edited_config, f)
    out_tests.test_write_config(edited_config)
    return ()

def import_database_columns(config, config_path="config.yaml"):
    """
    Get all database columns and write them to config as available to filters.
    """
    database = deepcopy(config["database"])
    in_tests.test_database_name(database)
    print (f"\nUpdating {config_path}")
    columns = {}
    for table in config["tables"].values():
        columns[table] = get_table_columns_names(database, table)
    config["filters_columns"] = columns
    write_config(config)
    return (columns)
