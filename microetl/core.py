########################################################
#    Name: ETLEng
# Release: 0.0.1
# Purpose: Minimal Data Transformation ETL Engine
#  Author: Paolo Fabio Zaino 
#   Usage: Check docs/ETLEng.md
########################################################

# Import Libraries
import os
import sys
sys.path.append("./")
sys.path.append("./dbconn")
import logging
import traceback
import datetime
import time
import re

# Import pandas and numpy libraries
# to read and write data in various formats
# and produce our ETL pipeline:
import pandas as pd
import numpy as np

# Import jsonbender library to transform JSON data
import jsonbender
from jsonbender import bend, K, S, F, OptionalS, If, Switch, Alternation, Forall, list_ops, Reduce, Filter, FlatForall, Format

# Import jsonschema library to validate JSON data
# and JSON to read and write JSON data
import json
import jsonschema
from jsonschema import validate
from jsonschema.exceptions import ValidationError
# TODO: import boto3

# Import yaml library to read and write YAML data
import yaml
from yamlinclude import YamlIncludeConstructor

# Import jinja2 library to allow for templating
from jinja2.environment import Template, Environment
from jinja2.runtime import Undefined 
from jinja2.loaders import DictLoader

# Import jinjaSQL library to allow for parameterized SQL queries
from jinjasql import JinjaSql

import pyjq

#add the following from graphql_compiler import graphql_to_sql

# Use dbconn.py to connect to a generic DB
# and execute SQL queries
import microetl.dbconn.interface as dbc

# import the abstracted Web API client
import microetl.apiclient as apic

# Globals
debug_level = 1
base_path: str = os.path.dirname(os.path.realpath(__file__))
cfg_path: str = os.path.join(base_path, 'jobs')
inp_path: str = os.path.join(base_path, 'inp_data')
out_path: str = os.path.join(base_path, 'out_data')
tmp_path: str = os.path.join(base_path, 'tmp_data')
log_path: str = os.path.join(base_path, 'logs')

# Error Messages
err_msg = [
    # 0. Generic error message:
    "Error in ETLEng: {}",
    # 1. Error message for invalid JSON Schema:
    "Error in ETLEng: Error reading the JSON Schema: ",
    # 2. Error message for invalid JSON Data:
    "Error in ETLEng: Invalid JSON Data: ",
    # 3. Error message for invalid YAML Configuration:
    "Error in ETLEng: Invalid YAML Configuration: ",
    # 4. Error message for invalid SQL Query:
    "Error in ETLEng: Invalid SQL Query: ",
    # 5. Error message for invalid SQL Parameters:
    "Error in ETLEng: Invalid SQL Parameters: ",
    # 6. Error message for invalid SQL Query File:
    "Error in ETLEng: Invalid SQL Query File: ",
    # 7. Error reading data from the Database:
    "Error in ETLEng: Error reading data from the Database: {}",
    # 8. Error writing data to the Database:
    "Error in ETLEng: Error writing data to the Database: {}",
    # 9. Error transforming data:
    "Error in ETLEng: Error transforming data: ",
    # 10. Error in validating data:
    "Error in ETLEng: Error in validating data: ",
    # 11. Error in validating a data item:
    "Error in ETLEng: Error in validating a data item: ",
    # 12. Error writing JSON file:
    "Error in ETLEng: Error writing data to the JSON file: ",
    # 13. Error running the ETL engine:
    "Error in running the ETL engine: ",
    # 14. Error running the ETL pipeline:
    "Error in running the ETL pipeline: ",
]

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Set the log level to DEBUG:
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

# Set the log level to WARNING:
# logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')

# Set the log level to ERROR:
# logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')

# Set the log level to CRITICAL:
# logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s %(levelname)s %(message)s')

# Function that does nothing:
def do_nothing():
    """
    Do nothing
    :return: None
    """
    logging.debug("Not implemented yet!")

# Function to process a variable containing a pyval:
def process_pyexpr(var):
    """
    Process a variable containing a pyexpr
    :param var: Python expression
    :return: Processed variable
    """
    try:
        pyval_prefix: str = 'pyexpr('
        pyval_suffix: str = ')'
        var = var.strip()
        tmp_var = var.lower()
        if tmp_var.startswith(pyval_prefix) and var.endswith(pyval_suffix):
            return str(eval(var[7:-1]))
        else:
            return var
    except Exception as e:
        logging.error(err_msg[0].format(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return var

# Generic function that reads a file:
def read_file(filename):
    """
    Read a file
    :param filename: Filename
    :return: File content
    """
    try:
        if filename != None and filename != '':
            with open(filename, 'r') as file:
                file_content = file.read()
            return file_content
        else:
            return None
    except Exception as e:
        logging.error(err_msg[3] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None
    
# Function to read a JSON Schema
def read_json_schema(json_schema_file):
    """
    Read the JSON Schema file
    :param json_schema_file: JSON Schema File
    :return: JSON Schema
    """
    try:
        with open(json_schema_file, 'r') as json_schema:
            json_schema = json.load(json_schema)
            return json_schema
    except Exception as e:
        logging.error(err_msg[1] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that reads and return a Configuration File (aka a Job file):
def read_config_file(config_file):
    """
    Read the Configuration File
    :param config_file: Configuration File
    :return: Configuration
    """
    try:
        with open(config_file, 'r') as file:
            config_raw = file.read()
        t = Template(config_raw, undefined=Undefined)
        config = yaml.load(t.render(os.environ), Loader=yaml.FullLoader)

        if debug_level > 1:
          test = json.dumps(config, indent=2)
          print("-- Config (in read_config_file): ")
          print(test)
          print("--")

        return config
    except Exception as e:
        logging.error(err_msg[3] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that takes microETL config, a datasource type and the input data path and returns data 
# as required by the configuration
def read_data_from_ds(config, datasource_type: str = 'file', section_name: str = 'source'):
    """
    Read data from the datasource
    :param config: Configuration
    :param datasource_type: Datasource Type
    :return: processed data
    """
    try:
        if section_name == "source":
            input_data_path: str = config.get("paths").get("inp_path")
        elif section_name == "destination":
            input_data_path: str = config.get("paths").get("out_path")
        elif section_name == "transform":
            input_data_path: str = config.get("paths").get("inp_path")
        else:
            input_data_path: str = config.get("paths").get("base_path")

        if datasource_type.lower().strip() == 'csv':
            data = pd.DataFrame()
            for filename in os.listdir(input_data_path):
                if filename.lower().endswith('.csv'):
                    full_filename: str = os.path.join(input_data_path, filename)
                    data += process_data(config, input_data_path, section_name, config.get("actions").get(section_name), pd.read_csv(full_filename)) 
        elif datasource_type.lower().strip() == 'json':
            data = json.loads('')
            for filename in os.listdir(input_data_path):
                if filename.lower().endswith('.json'):
                    full_filename: str = os.path.join(input_data_path, filename)
                    data += process_data(config, input_data_path, section_name, config.get("actions").get(section_name), pd.read_json(full_filename))
        elif datasource_type.lower().strip() == 'excel':
            data = pd.DataFrame()
            for filename in os.listdir(input_data_path):
                if filename.lower().endswith('.xlsx'):
                    full_filename: str = os.path.join(input_data_path, filename)
                    data += process_data(config, input_data_path, section_name, config.get("actions").get(section_name), pd.read_excel(full_filename))
        elif datasource_type.lower().strip() == 'api':
            data = process_data(config, input_data_path, section_name, config.get("actions").get(section_name), read_data_from_api(config, section_name))
        elif datasource_type.lower().strip() == 'file':
            data = ''
            for filename in os.listdir(input_data_path):
                if filename != None and filename != '': 
                    full_filename: str = os.path.join(input_data_path, filename)
                    tmp_data = process_data(config, input_data_path, section_name, config.get("actions").get(section_name), read_data_from_file(full_filename))
                    if tmp_data != None and tmp_data != '':
                        data += str(tmp_data)
        else:
            ds_type = datasource_type.lower().strip()
            db_list = ['snowflake', 'mysql', 'postgresql', 'neo4j', 'elasticsearch', 'mongodb']
            if any(ds_type in s for s in db_list):
                data = get_data_from_db(config, input_data_path, datasource_type, section_name)
            else:
                raise ValueError("Invalid datasource type: " + datasource_type)
        
        return data
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function that reads a generic data file:
def read_data_from_file(filename: str = 'data.txt'):
    """
    Read the Configuration File
    :param filename: Configuration File
    :return: Raw data
    """
    try:
        if debug_level > 0:
            print("Reading data from raw data file: " + filename + " ... ")
        with open(filename, 'r') as file:
            data = file.read()
        return data
    except Exception as e:
        logging.error(err_msg[3] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function that takes microETL config, and returns data from an API
def read_data_from_api(config, section_name):
    """
    Read data from an API
    :param config: Configuration
    :param input_data_path: Input Data Path
    :return: Data
    """
    try:
        if config.get('datasources').get(section_name).get('api').get('method') == 'restful_get':
            data = apic.get_data(config.get('datasources').get(section_name).get('api').get('request'), None, None)
        elif config.get('datasources').get(section_name).get('api').get('method') == 'restful_post':
            data = apic.post_data(config.get('datasources').get(section_name).get('api').get('request'), None, None)
        elif config.get('datasources').get(section_name).get('api').get('method') == 'soap':
            data = apic.get_soap_data(config.get('datasources').get(section_name).get('api').get('request'))
        else:
            raise ValueError(err_msg[15] + config.get('datasources').get(section_name).get('api').get('method') + " is not a valid API method")
        return data
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that takes microETL config, a datasource type and returns data from a database
# and uses dbc to connect to the database and execute queries
def get_data_from_db(config, inp_path, datasource_type: str = 'postgres', section_name: str = 'source'):
    """
    Read data from a database
    :param config: Configuration
    :param inp_path: Input Data Path
    :param datasource_type: Datasource Type
    :param section_name: Section Name
    :return: Data
    """
    try:
        # Get the datasource type
        ds_type = datasource_type.lower().strip()
        if ds_type == 'sql':
            raise ValueError(datasource_type + " is not a valid datasource type")
        
        # Get the query to run
        query = config.get('actions').get(section_name).get('template')
        if query == None or query == '':
            query = config.get('actions').get(section_name).get('query')
        else:
            query = read_sql_query(os.path.join(inp_path, query))

        if query == None or query == '':
            raise ValueError("Invalid query: '" + str(query) + "' for section: " + section_name + ". template cannot be empty!")

        # Get the query parameters
        query_params = config.get('actions').get(section_name).get('query_params')
        if query_params == None or query_params == '':
            query_params = config.get('actions').get(section_name).get('query_parameters')
        if query_params == None or query_params == '':
            query_params = config.get('actions').get(section_name).get('parameters')
        if query_params == None or query_params == '':
            query_params = config.get('actions').get(section_name).get('params')

        if debug_level > 1:
            print("-- Query params (in read_data_from_db): ")
            print(query_params)
            print("--")

        # Process query and parameters
        query, query_params = replace_sql_parameters(config, inp_path, query, section_name, query_params)

        if debug_level > 1:
            print("-- Query processed (in read_data_from_db): ")
            print(query)
            print("--")
        
        if query != None and query != '':
            # Get the database connection
            conn = dbc.get_db_connection(config, section_name)

            # Get the Cursor
            cur = dbc.get_db_cursor(conn, ds_type)

            # Get the type of action to perform
            action_type = config.get('actions').get(section_name).get('type').lower().strip()

            if debug_level > 1:
                print("-- Action type (in read_data_from_db): ")
                print(action_type)
                print("--")

            # Execute the query
            if action_type == 'sql_to_dataframe':
                data = dbc.execute_db_query_return_dataframe(conn, cur, query, ds_type, query_params)
            elif action_type == 'sql_to_results':
                data = dbc.execute_db_query_return_results(conn, cur, query, ds_type, query_params)
            elif action_type == 'sql_to_json':
                data = dbc.execute_db_query_return_json(conn, cur, query, ds_type, query_params)
            else:
                data = None

            # Close the cursor
            dbc.close_db_cursor(cur, ds_type)

            # Close the connection
            dbc.close_db_connection(conn, ds_type)

            return data
        else:
            return None
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function to read an SQL Query file and return the Query
def read_sql_query(sql_query_file):
    """
    Read the SQL Query File
    :param sql_query_file: SQL Query File
    :return: SQL Query
    """
    try:
        with open(sql_query_file, 'r') as sql_query:
            sql_query = sql_query.read()
            return sql_query
    except Exception as e:
        logging.error(err_msg[6] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that takes an SQL query and a dictionary of parameters and returns a SQL query with the parameters replaced
def replace_sql_parameters(config, inp_path, sql_query, section_name, parameters):
    """
    Replace the SQL Parameters
    :param config: Configuration
    :param sql_query: SQL Query
    :param section_name: Section Name of the config where to find the parameters
    :param parameters: Parameters
    :return: SQL Query with the Parameters replaced
    """
    try:
        # Retrieve parameters from the configuration file
        if parameters == None or parameters == []:
            parameters = config.get('actions').get(section_name).get('parameters')

        #if parameters != None and parameters != []:
            # Loop through the parameters
            #for parameter in parameters:
            #    if parameter.get('name').lower().strip() == 'parameters_file' or parameter.get('name').lower().strip() == 'file':
            #        if parameter.get('value') != None:
            #            par_filename = os.path.join(inp_path,  parameter['value']) 
            #            with open(par_filename, 'r') as par_file:
            #                par_data = yaml.safe_load(par_file)
            #                parameters.update(par_data)
            #        else:
            #            for idx, obj in enumerate(parameters):
            #                if obj['name'] == parameter.get('name'):
            #                    parameters.pop(idx)

        if debug_level > 1:
            print("-- Parameters processed (in replace_sql_parameters): ")
            print(parameters)
            print("--")

        if parameters is None or parameters == []:
            return sql_query

        # Create a dictionary of parameters
        pyval_prefix = 'pyval('
        pyval_suffix = ')'
        par_dict = {}
        for idx, obj in enumerate(parameters):
            res = obj['name']
            if obj['name'].lower().strip().startswith(pyval_prefix):
                par = obj['name']
                res = ''.join(par.split(pyval_prefix)[1].split(pyval_suffix)[0])
                val = str(eval(obj['value'])).strip().replace("'", "").replace('"', '').replace(' ', '')
                parameters[idx]['value'] = val
            par_dict[res]=obj['value']
            
        yaml_dict = yaml.safe_load(str(par_dict))

        if debug_level > 1:
            print("-- Parameters dictionary (in replace_sql_parameters): ")
            print(yaml_dict)
            print("--")

        # Create a JinjaSql object
        loader = DictLoader({"params" : yaml_dict})

        if debug_level > 1:
            print("-- Loader (in replace_sql_parameters): ")
            print(loader.mapping)
            print("--")

        j = JinjaSql(param_style='pyformat')
        query, bind_params = j.prepare_query(j.env.from_string(sql_query), loader.mapping)

        if debug_level > 1:
            print("-- Query (in replace_sql_parameters): ")
            print(query)
            print("--")
            print("-- Params (in replace_sql_parameters): ")
            print(bind_params)
            print("--")

        return query, bind_params
    except Exception as e:
        logging.error(err_msg[5] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function that takes a configuration and a dictionary of parameters and returns a configuration with the parameters replaced or added
def process_config_parameters(config, inp_path, section_name, parameters):
    """
    Process the Configuration Parameters
    :param config: Configuration
    :param section_name: Section Name of the config where to find the parameters
    :param parameters: Parameters
    :return: Configuration with the Parameters replaced or added
    """
    try:
        tmp_pars = parameters
        if parameters != None:
            # Loop through the parameters
            for parameter in parameters:
                if parameter.get('name').lower().strip() == 'parameters_file' or parameter.get('name').lower().strip() == 'file':
                    if parameter.get('value') != None:
                        par_filename = os.path.join(inp_path,  parameter['value']) 
                        with open(par_filename, 'r') as par_file:
                            par_data = yaml.safe_load(par_file)
                            parameters.update(par_data)
        
            # Create a Jinja2 Environment
            env = Environment(undefined=Undefined)
            # Create a Jinja2 Template
            template = env.from_string(str(parameters))
            # Render the Template
            tmp_pars = yaml.safe_load(template.render(os.environ))

        if debug_level > 1:
            print("-- Processed parameters (in process_config_parameters): ")
            print(tmp_pars)
            print("--")

        return tmp_pars
    except Exception as e:
        logging.error(err_msg[4] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return parameters

# Function that takes a configuration and a data unit and processes it according to the configuration
def process_data(config, inp_path, sect_name, action, data):
    """
    Process the Data
    :param config: Configuration
    :param inp_path: Input Path
    :param sect_name: Section Name (within the action) of the config where to find the parameters
    :param action: Action (a specific section of the job config)
    :param data: Data
    :return: Processed Data
    """
    try:
        # Get the Actions
        #actions = config.get('actions').get(sect_name)
        # Get the Action Type
        action_type = action.get('type').lower().strip()
        if action_type == None:
            return data
        
        # Get the Action Parameters
        parameters = action.get('parameters')
        # Get the Action Parameters
        parameters = process_config_parameters(config, inp_path, sect_name, parameters)

        # Process the Action
        if action_type == 'filter':
            data = filter_data(data, parameters)
        elif action_type == 'aggregate':
            data = aggregate_data(data, parameters)
        elif action_type == 'sort':
            data = sort_data(data, parameters)
        elif action_type == 'pivot':
            data = pivot_data(data, parameters)
        elif action_type == 'join':
            data = join_data(data, parameters)
        elif action_type == 'write':
            write_data_to_ds(config, action.get('location'), data, parameters)
        elif action_type == 'read':
            data = read_data_from_ds(config, action.get('location'), sect_name)
        elif action_type == 'sql':
            query, query_params = replace_sql_parameters(config, inp_path, read_sql_query(parameters.get('sql_query_file')), sect_name, parameters)
            data = read_data_from_db(config, query, query_params)
        elif action_type == 'dsl':
            data = transform_data_json_to_json(data, read_file(os.path.join(process_pyexpr(action.get("template_path", '')), action.get("template"))), read_file(os.path.join(process_pyexpr(action.get("schema_path", '')), action.get("schema", ''))))
        elif action_type == 'jq':
            data = transform_data_json_to_json_pyjq(data, read_file(os.path.join(process_pyexpr(action.get("template_path", '')), action.get("template"))), read_file(os.path.join(process_pyexpr(action.get("schema_path", '')), action.get("schema", ''))))
        elif action_type == 'print':
            print("-- Data (in process_data):")
            print(data)
            print("--")
        elif action_type == 'api':
            data = read_data_from_api(config, parameters.get('input_data_path'))
        else:
            logging.error(err_msg[14] + "Invalid action type: " + str(action_type))
            return data
        
        return data
    except Exception as e:
        logging.error(err_msg[12] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function to filter the Data according to the parameters
def filter_data(data, parameters):
    """
    Filter the Data
    :param data: Data
    :param parameters: Parameters
    :return: Filtered Data
    """
    try:
        # Get the Filter Parameters
        filter_parameters = parameters.get('filter_parameters')
        # Loop through the Filter Parameters
        for filter_parameter in filter_parameters:
            # Get the Filter Parameter
            filter_parameter = filter_parameters.get(filter_parameter)
            # Get the Filter Parameter Name
            filter_parameter_name = filter_parameter.get('filter_parameter_name')
            # Get the Filter Parameter Value
            filter_parameter_value = filter_parameter.get('filter_parameter_value')
            # Get the Filter Parameter Operator
            filter_parameter_operator = filter_parameter.get('filter_parameter_operator')
            # Filter the Data
            if filter_parameter_operator == 'eq':
                data = data[data[filter_parameter_name] == filter_parameter_value]
            elif filter_parameter_operator == 'ne':
                data = data[data[filter_parameter_name] != filter_parameter_value]
            elif filter_parameter_operator == 'gt':
                data = data[data[filter_parameter_name] > filter_parameter_value]
            elif filter_parameter_operator == 'ge':
                data = data[data[filter_parameter_name] >= filter_parameter_value]
            elif filter_parameter_operator == 'lt':
                data = data[data[filter_parameter_name] < filter_parameter_value]
            elif filter_parameter_operator == 'le':
                data = data[data[filter_parameter_name] <= filter_parameter_value]
            elif filter_parameter_operator == 'in':
                data = data[data[filter_parameter_name].isin(filter_parameter_value)]
            elif filter_parameter_operator == 'not in':
                data = data[~data[filter_parameter_name].isin(filter_parameter_value)]
            elif filter_parameter_operator == 'contains':
                data = data[data[filter_parameter_name].str.contains(filter_parameter_value)]
            elif filter_parameter_operator == 'not contains':
                data = data[~data[filter_parameter_name].str.contains(filter_parameter_value)]
            elif filter_parameter_operator == 'startswith':
                data = data[data[filter_parameter_name].str.startswith(filter_parameter_value)]
            elif filter_parameter_operator == 'endswith':
                data = data[data[filter_parameter_name].str.endswith(filter_parameter_value)]
            else:
                logging.error(err_msg[15] + "Invalid filter operator: " + filter_parameter_operator)
                sys.exit(1)
        return data
    except Exception as e:
        logging.error(err_msg[11] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None
    
# Function to aggregate the Data according to the parameters
def aggregate_data(data, parameters):
    """
    Aggregate the Data
    :param data: Data
    :param parameters: Parameters
    :return: Aggregated Data
    """
    try:
        # Get the Aggregate Parameters
        aggregate_parameters = parameters.get('aggregate_parameters')
        # Get the Aggregate Parameter
        aggregate_parameter = aggregate_parameters.get('aggregate_parameter')
        # Get the Aggregate Parameter Name
        aggregate_parameter_name = aggregate_parameter.get('aggregate_parameter_name')
        # Get the Aggregate Parameter Function
        aggregate_parameter_function = aggregate_parameter.get('aggregate_parameter_function')
        # Get the Aggregate Parameter Group By
        aggregate_parameter_group_by = aggregate_parameter.get('aggregate_parameter_group_by')
        # Aggregate the Data
        if aggregate_parameter_function == 'count':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].count().reset_index()
        elif aggregate_parameter_function == 'sum':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].sum().reset_index()
        elif aggregate_parameter_function == 'mean':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].mean().reset_index()
        elif aggregate_parameter_function == 'median':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].median().reset_index()
        elif aggregate_parameter_function == 'min':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].min().reset_index()
        elif aggregate_parameter_function == 'max':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].max().reset_index()
        elif aggregate_parameter_function == 'std':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].std().reset_index()
        elif aggregate_parameter_function == 'var':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].var().reset_index()
        elif aggregate_parameter_function == 'first':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].first().reset_index()
        elif aggregate_parameter_function == 'last':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].last().reset_index()
        elif aggregate_parameter_function == 'nunique':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].nunique().reset_index()
        elif aggregate_parameter_function == 'unique':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].unique().reset_index()
        elif aggregate_parameter_function == 'list':
            data = data.groupby(aggregate_parameter_group_by)[aggregate_parameter_name].apply(list).reset_index()
        else:
            logging.error(err_msg[16] + "Invalid aggregate function: " + aggregate_parameter_function)
            return None
        return data
    except Exception as e:
        logging.error(err_msg[12] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function to sort the Data according to the parameters
def sort_data(data, parameters):
    """
    Sort the Data
    :param data: Data
    :param parameters: Parameters
    :return: Sorted Data
    """
    try:
        # Get the Sort Parameters
        sort_parameters = parameters.get('sort_parameters')
        # Get the Sort Parameter
        sort_parameter = sort_parameters.get('sort_parameter')
        # Get the Sort Parameter Name
        sort_parameter_name = sort_parameter.get('sort_parameter_name')
        # Get the Sort Parameter Order
        sort_parameter_order = sort_parameter.get('sort_parameter_order')
        # Sort the Data
        if sort_parameter_order == 'asc':
            data = data.sort_values(by=[sort_parameter_name], ascending=True)
        elif sort_parameter_order == 'desc':
            data = data.sort_values(by=[sort_parameter_name], ascending=False)
        else:
            logging.error(err_msg[17] + "Invalid sort order: " + sort_parameter_order)
            return None
        return data
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function to limit the Data according to the parameters
def limit_data(data, parameters):
    """
    Limit the Data
    :param data: Data
    :param parameters: Parameters
    :return: Limited Data
    """
    try:
        # Get the Limit Parameters
        limit_parameters = parameters.get('limit_parameters')
        # Get the Limit Parameter
        limit_parameter = limit_parameters.get('limit_parameter')
        # Get the Limit Parameter Value
        limit_parameter_value = limit_parameter.get('limit_parameter_value')
        # Limit the Data
        data = data.head(limit_parameter_value)
        return data
    except Exception as e:
        logging.error(err_msg[14] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None
    
# Function that pivot data according to the parameters
def pivot_data(data, parameters):
    """
    Pivot the Data
    :param data: Data
    :param parameters: Parameters
    :return: Pivoted Data
    """
    try:
        # Get the Pivot Parameters
        pivot_parameters = parameters.get('pivot_parameters')
        # Get the Pivot Parameter
        pivot_parameter = pivot_parameters.get('pivot_parameter')
        # Get the Pivot Parameter Name
        pivot_parameter_name = pivot_parameter.get('pivot_parameter_name')
        # Get the Pivot Parameter Values
        pivot_parameter_values = pivot_parameter.get('pivot_parameter_values')
        # Get the Pivot Parameter Index
        pivot_parameter_index = pivot_parameter.get('pivot_parameter_index')
        # Pivot the Data
        data = data.pivot(index=pivot_parameter_index, columns=pivot_parameter_name, values=pivot_parameter_values)
        return data
    except Exception as e:
        logging.error(err_msg[15] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function that joins data according to the parameters
def join_data(data, parameters):
    """
    Join the Data
    :param data: Data
    :param parameters: Parameters
    :return: Joined Data
    """
    try:
        # Get the Join Parameters
        join_parameters = parameters.get('join_parameters')
        # Get the Join Parameter
        join_parameter = join_parameters.get('join_parameter')
        # Get the Join Parameter Name
        join_parameter_name = join_parameter.get('join_parameter_name')
        # Get the Join Parameter Values
        join_parameter_values = join_parameter.get('join_parameter_values')
        # Get the Join Parameter Index
        join_parameter_index = join_parameter.get('join_parameter_index')
        # Join the Data
        data = data.join(join_parameter_values, on=join_parameter_index, how=join_parameter_name)
        return data
    except Exception as e:
        logging.error(err_msg[15] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function that prints data
def print_data(data):
    """
    Print the Data
    :param data: Data
    :return: None
    """
    try:
        # Print the Data
        print(data)

    except Exception as e:
        logging.error(err_msg[16] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# This function reads data as an argument and transforms it according to the provided MicroETL configuration
def transform_data(config, data, section_name: str ="transform"):
    """
    Transform the Data
    :param config: Configuration
    :param data: Data
    :param section_name: Section Name
    :return: Transformed Data
    """
    try:
        # Get the Transform Section
        transform_section = config.get('actions').get(section_name)
        # Get the Transform sequence
        transform_sequence = transform_section.get('sequence')

        # Loop through the Transform sequence
        for transform in transform_sequence:
            if debug_level > 1:
              print(yaml.dump(transform))
            # Get subsection
            #transform_step = transform.get('step')
            # Get the Transform name
            transform_type: str = transform.get("type").lower().strip()
            # Process the Data
            data = process_data(config, transform.get('inp_path'), transform_type, transform, data)
            # Check if the data is empty
            if data == None:
                logging.error(err_msg[0].format("Empty data after transform: " + transform_type))
                return None
            
        return data
    except Exception as e:
        logging.error(err_msg[0].format(str(e)))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return data


# Function that writes data to a datasource according to the provided MicroETL configuration
def write_data_to_ds(config, out_path, data, parameters):
    """
    Write the Data
    :param config: Configuration
    :param data: Data
    :param parameters: Parameters
    :return: None
    """
    try:
        # Get the Write Parameters
        write_parameters = config.get('datasources').get('destination') 
        # Get the Write Parameter
        write_parameter = write_parameters.get('write_parameter')
        # Get the Write Parameter Name
        write_parameter_name = write_parameter.get('write_parameter_name')
        # Get the Write Parameter Value
        write_parameter_value = write_parameter.get('write_parameter_value')
        # Get the Write Parameter Type
        write_parameter_type = write_parameter.get('write_parameter_type')
        # Write the Data
        if write_parameter_type == 'file':
            #write_data_to_file(config, data, write_parameter_value)
            do_nothing()
        elif write_parameter_type == 'db':
            #write_data_to_db(config, data, write_parameter_value)
            do_nothing()
        else:
            logging.error(err_msg[18] + "Invalid write type: " + write_parameter_type)
            return None
    except Exception as e:
        logging.error(err_msg[19] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function to read the Data from a Database
# and return a DataFrame
def read_data_from_db(config, sql_query, sql_params=None):
    """
    Read the Data from the Database
    :param config: Configuration
    :param sql_query: SQL Query
    :param sql_params: SQL Parameters
    :return: Data
    """
    try:
        # Get the DB Connection
        db_connection = dbc.get_db_connection(*config)
        # Get the DB Cursor
        db_cursor = dbc.get_db_cursor(db_connection, config['db_type'])
        # Execute the SQL Query and get the Data
        data = dbc.execute_db_query_return_dataframe(db_connection, db_cursor, sql_query, config['db_type'], sql_params)
        # Close the DB Connection
        #if config['db_type'] != 'none':
        #    db_connection.close()
        return data
    except Exception as e:
        logging.error(err_msg[7].format(str(e)))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)


# Function that transform the Data from the old JSON schema to the new JSON schema
# and return the transformed Data as a DataFrame
def transform_data_old_to_new(data, old_json_schema, json_schema):
    """
    Transform the Data from the old JSON schema to the new JSON schema
    :param data: Data from the Database
    :param old_json_schema: Old JSON Schema
    :param json_schema: New JSON Schema
    :return: Transformed Data
    """
    try:
        # Create a new DataFrame
        new_data = pd.DataFrame()
        # Iterate through each column in the old JSON Schema
        for column in old_json_schema['properties']:
            # Check if the column is present in the new JSON Schema
            if column in json_schema['properties']:
                # Check if the column is present in the data
                if column in data.columns:
                    # Check if the column is of type array
                    if old_json_schema['properties'][column]['type'] == 'array':
                        # Iterate through each element in the array
                        for element in old_json_schema['properties'][column]['items']['properties']:
                            # Check if the element is present in the new JSON Schema
                            if element in json_schema['properties'][column]['items']['properties']:
                                # Check if the element is present in the data
                                if element in data.columns:
                                    # Check if the element is of type array
                                    if old_json_schema['properties'][column]['items']['properties'][element]['type'] == 'array':
                                        # Iterate through each element in the array
                                        for sub_element in old_json_schema['properties'][column]['items']['properties'][element]['items']['properties']:
                                            # Check if the sub element is present in the new JSON Schema
                                            if sub_element in json_schema['properties'][column]['items']['properties'][element]['items']['properties']:
                                                # Check if the sub element is present in the data
                                                if sub_element in data.columns:
                                                    # Check if the sub element is of type array
                                                    if old_json_schema['properties'][column]['items']['properties'][element]['items']['properties'][sub_element]['type'] == 'array':
                                                        # Iterate through each element in the array
                                                        for sub_sub_element in old_json_schema['properties'][column]['items']['properties'][element]['items']['properties'][sub_element]['items']['properties']:
                                                            # Check if the sub sub element is present in the new JSON Schema
                                                            if sub_sub_element in json_schema['properties'][column]['items']['properties'][element]['items']['properties'][sub_element]['items']['properties']:
                                                                # Check if the sub sub
                                                                if sub_sub_element in data.columns:
                                                                    # Add the sub sub element to the new DataFrame
                                                                    new_data[sub_sub_element] = data[sub_sub_element]
                                                    else:
                                                        # Add the sub element to the new DataFrame
                                                        new_data[sub_element] = data[sub_element]
                                    else:
                                        # Add the element to the new DataFrame
                                        new_data[element] = data[element]
                    else:
                        # Add the column to the new DataFrame
                        new_data[column] = data[column]
        return new_data
    except Exception as e:
        logging.error(err_msg[9] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that transform the data from JSON format to JSON format using jsonbender 
# and return the transformed Data as a JSON object
def transform_data_json_to_json(data, mapping, json_schema = None):
    """
    Transform the Data from JSON format to JSON format using jsonbender
    :param data: Data from the Database (in JSON format!)
    :param mapping: Mapping (a DSL for transforming JSON objects)
    :param json_schema: JSON Schema
    :return: Transformed Data
    """
    try:
        # Process the mapping. 
        # Paolo's note: this is a micro parser for the DSL
        #               when using DSL from a text file!
        if isinstance(mapping, str):
            processed_map = {}
            pattern = r"^.*[\'|\"]{1}(?P<key>[a-zA-Z0-9]+)[\'|\"]{1}\s*[\:]{1}\s*(?P<value>.*)\s*[,]{1}\s*$"
            line = ''
            for char in mapping:
                if char == '\n':
                    if line.strip() != '':
                        matches = re.match(pattern, line, flags=re.I|re.M|re.U)
                        if matches:
                            processed_map[matches.group("key")] = eval(matches.group("value"))
                    line = ''
                else:
                    line += char
        else:
            processed_map = mapping

        # Remap the data:    
        new_data = bend(processed_map, data)

        if json_schema != None:
            valid = validate_data(new_data, json_schema)
            if not valid:
                raise ValidationError("The data is not valid according to the JSON Schema")
            
        return new_data
    except Exception as e:
        logging.error(err_msg[10] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return data

# This function transforms the data from JSON format "A" to JSON format "B" using pyjq
# and a simple mapping of JSON "A" to JSON "B" and return the transformed Data as a JSON object
def transform_data_json_to_json_pyjq(data, mapping, json_schema):
    """
    Transform the Data from JSON format to JSON format using pyjq
    :param data: Data from the Database (in JSON format!)
    :param mapping: Mapping (a DSL for transforming JSON objects)
    :param json_schema: JSON Schema
    :return: Transformed Data
    """
    try:
        # if either data or mapping is None, skip this "step"
        if data == None or mapping == None:
            return data
        
        # Process the mapping
        # TBD

        tmp_data = '{ "source": ' + str(data) + '}'
        print("tmp_data: " + tmp_data)
        data_json = json.loads( tmp_data )
        print(json.dumps(data_json, indent=4))

        # Transform the data
        new_data = pyjq.all(mapping, data_json)

        # Validate the data if a JSON Schema is provided
        if json_schema != None and new_data != None:
            valid = validate_data(new_data, json_schema)
            if not valid:
                raise ValidationError("The data is not valid according to the JSON Schema")

        return new_data
    except Exception as e:
        logging.error(err_msg[10] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return data

# Function that transform a GraphQL query into an SQL query
#def transform_graphql_to_sql(graphql_query, sql_schema_info, parameters, db_type):
#    """
#    Transform a GraphQL query into an SQL query
#    :param graphql_query: GraphQL Query
#    :param sql_schema_info: SQL Schema information
#    :param parameters: Parameters (if any)
#    :return: SQL Query
#    """
#    try:
#        # Set the compiler metadata
#        compiler_metadata = {
#            "dialect": db_type,
#        }
#        # Transform the GraphQL query into an SQL query
#        sql_query = graphql_to_sql(sql_schema_info, graphql_query, parameters, compiler_metadata)
#        return sql_query
#    except Exception as e:
#        logging.error(err_msg[4] + str(e))
#        logging.error(err_msg[0].format(traceback.format_exc()))
#        sys.exit(1)

# Function that validates a JSON Schema
def validate_json_schema(json_schema):
    """
    Validate the JSON Schema
    :param json_schema: JSON Schema
    :return: Validated JSON Schema
    """
    try:
        # Validate the JSON Schema
        jsonschema.Draft202012Validator.check_schema(json_schema)
        return True
    except Exception as e:
        logging.error(err_msg[1] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return False

# function that validate a single item of data against a schema
def validate_data_item(data_item, json_schema):
    """
    Validate a single item of data against a schema
    :param data_item: Data Item
    :param json_schema: JSON Schema
    :return: Validated Data Item
    """
    try:
        # Validate the Data Item
        jsonschema.validate(data_item, json_schema)
        return data_item
    except Exception as e:
        logging.error(err_msg[11] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        # sys.exit(1)
        return None

# function that validate a list of data against a schema (using the validate_data_item function)
def validate_data(data, json_schema):
    """
    Validate a list of data against a schema
    :param data: Data
    :param json_schema: JSON Schema
    :return: Validated Data
    """
    try:
        # Validate the Schema
        if not validate_json_schema(json_schema):
            return False
        # Validate Data
        jsonschema.validate(data, json_schema)
        return True
    except ValidationError as e:
        logging.error(err_msg[2] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return False
    except Exception:
        return False

# Function to load a JSON object from a JSON file
def load_json_from_file(json_file):
    """
    Load a JSON object from a JSON file
    :param json_file: JSON File
    :return: JSON Object
    """
    try:
        # Load the JSON object from the JSON file
        with open(json_file, 'r') as json_data:
            return json.load(json_data)
    except Exception as e:
        logging.error(err_msg[2] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that writes the data to a JSON file
def write_data_to_json(data, json_file):
    """
    Write the Data to a JSON file
    :param data: Data
    :param json_file: JSON File
    :return: None
    """
    try:
        # Write the Data to a JSON file
        with open(json_file, 'w') as json_data:
            json.dump(data, json_data, indent=4)
    except Exception as e:
        logging.error(err_msg[12] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that takes an SQL query and return a JSON document following the JSON schema
def etleng_sql_to_json(config, section_name, json_schema_file, sql_query):
    """
    ETL Engine
    :param config: Configuration File
    :param section_name: Section Name
    :param json_schema_file: JSON Schema File
    :param sql_query: SQL Query
    :return: JSON Document
    """
    try:
        # Get DS type
        ds_type = config.get('datasources').get(section_name).get('type')
        if ds_type == None or ds_type == '':
            ds_type = config.get('datasources').get(section_name).get('db_type')
        if ds_type == None or ds_type == '':
            raise ValueError("Datasource type is not defined in the job file")
        
        # Connect to a Database
        conn = dbc.get_db_connection(config, section_name)
        # Get Cursor
        cur = dbc.get_db_cursor(conn, ds_type)
        # Run the SQL Query
        df = dbc.execute_db_query_return_dataframe(conn, cur, sql_query, ds_type)
        # Close the Cursor
        # dbc.
        # Close the Connection
        dbc.close_db_connection(conn, ds_type)
        # Transform the Dataframe into a JSON document
        return etleng_df_to_json(df, json_schema_file)
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return None

# Function that transform a dataframe into a JSON document following the JSON schema
def etleng_df_to_json(df, json_schema_file):
    """
    ETL Engine
    :param df: Dataframe
    :param json_schema_file: New JSON Schema File
    :return: A JSON document following the JSON schema
    """
    try:
        # Read the New JSON Schema
        json_schema = read_json_schema(json_schema_file)
        # Validate the Data
        data = validate_data(df.to_dict('records'), json_schema)
        # Write the Data to a JSON file
        return data
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that runs the ETL pipeline
def etleng_run_pipeline(config):
    """
    Run the ETL Pipeline
    :param config: Job configuration file
    :return: None
    """
    
    try:
        # Process configuration file
        inp_path: str = config.get('paths').get('inp_path')
        out_path: str = config.get('paths').get('out_path')
        src_ds: str = ''
        src_ds = str(config.get('datasources').get('source').get('type' , ''))
        if src_ds == '':
            src_ds = str(config.get('datasources').get('source').get('db_type' , ''))
        if src_ds == '' and inp_path != '':
            src_ds = 'file'
        if src_ds == '' and inp_path == '':
            raise ValueError('No source data type specified in the configuration file')
        
        dst_ds: str = ''
        dst_ds = str(config.get('datasources').get('destination').get('type' , ''))
        if dst_ds == '':
            dst_ds = str(config.get('datasources').get('destination').get('db_type' , ''))
        if dst_ds == '' and out_path != '':
            dst_ds = 'file'
        if dst_ds == '' and out_path == '':
            raise ValueError('No destination data type specified in the configuration file')

        # Read the Data from the Source
        data = read_data_from_ds(config, str(src_ds), "source")
        if debug_level > 0:
            print("-- Data from source (in etleng_run_pipeline):")
            print(data)
            print("--")

        # Transform the Data to the new format:
        if config.get("actions").get("transform") != None:
            data = transform_data(config, data, "transform")
            if debug_level > 0:
                print("-- Data from transform (in etleng_run_pipeline):")
                print(data)
                print("--")

        # if config['old_json_schema'] != '':
        #     data = transform_data_old_to_new(data, old_json_schema, json_schema)
        #     # Validate the Data
        #     data = validate_data(data.to_dict('records'), json_schema)
        # else:
        #     data = etleng_df_to_json(data, json_schema)

        # # Write the Data to a JSON file
        # write_data_to_json(data, json_file)
        return True
    except Exception as e:
        logging.error(err_msg[14] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return False

# Function that loads a pipeline configuration file and run the ETL pipeline
def etleng_run_pipeline_from_config(config_file, base_path: str, cfg_path: str, inp_path: str, out_path: str):
    """
    Run the ETL Pipeline from a Configuration File
    :param config_file: Configuration File
    :param cfg_path: Configuration Path
    :param inp_path: Input Data path (if any)
    :param out_path: Output Data path (if any)
    :return: None
    """
    try:
        if debug_level > 0:
            print('Loading config file: ' + config_file)

        # Add the !include constructor to the yaml loader
        # which will use for base_path the parent path to the directory that contains 
        # the jobs configuration file
        YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader, base_dir=base_path)

        # Read the Configuration File
        config = read_config_file(os.path.join(cfg_path, config_file))
        # Check if the input path is provided in the configuration file
        if config.get('datasources').get('source').get('local_input_data') != None:
            inp_path = str(config.get('datasources').get('source').get('local_input_data'))
        # Check if the output path is provided in the configuration file
        if config.get('datasources').get('destination').get('local_output_data') != None:
            out_path = str(config.get('datasources').get('destination').get('local_output_data'))

        paths = {
                "base_path": base_path,
                "cfg_path": cfg_path,
                "inp_path": inp_path,
                "out_path": out_path
        }

        config["paths"] = paths

        if debug_level > 1:
            print(yaml.dump(config, default_flow_style=False))

        # Run the ETL Pipeline
        return etleng_run_pipeline(config)

    except Exception as e:
        logging.error(err_msg[15] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        return False
