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
#importa jsonbender
from jsonbender import bend, K, S, F, OptionalS, If, Switch, Alternation, Forall, list_ops, Reduce, Filter, FlatForall, Format

# Import jsonschema library to validate JSON data
# and JSON to read and write JSON data
import json
import jsonschema
from jsonschema import validate
from jsonschema.exceptions import ValidationError
#importa boto3

# Import yaml library to read and write YAML data
import yaml

# Import jinjaSQL library to allow for parameterized SQL queries
from jinjasql import JinjaSql

# Import jinja2 library to allow for templating
from jinja2 import Template, Undefined

#add the following from graphql_compiler import graphql_to_sql

# Use dbconn.py to connect to a generic DB
# and execute SQL queries
from dbconn import interface as dbc

# Globals
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

# Function to read and return a Configuration File
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
        config = yaml.safe_load(t.render(os.environ))
        return config
    except Exception as e:
        logging.error(err_msg[3] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

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
def replace_sql_parameters(sql_query, parameters):
    """
    Replace the SQL Parameters
    :param sql_query: SQL Query
    :param parameters: Parameters
    :return: SQL Query with the Parameters replaced
    """
    try:
        # Create a JinjaSql object
        j = JinjaSql()
        # Create a new SQL Query with the parameters replaced
        new_sql_query, bind_params = j.prepare_query(sql_query, parameters)
        return new_sql_query
    except Exception as e:
        logging.error(err_msg[5] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function to read the Data from a Database
# and return a DataFrame
def read_data_from_db(config, sql_query):
    """
    Read the Data from the Database
    :param config: Configuration
    :param sql_query: SQL Query
    :return: Data
    """
    try:
        # Get the DB Connection
        db_connection = dbc.get_db_connection(*config)
        # Get the DB Cursor
        db_cursor = dbc.get_db_cursor(db_connection, config['db_type'])
        # Execute the SQL Query and get the Data
        data = dbc.execute_db_query_return_dataframe(db_connection, db_cursor, sql_query, config['db_type'])
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
def transform_data_json_to_json(data, mapping, json_schema):
    """
    Transform the Data from JSON format to JSON format using jsonbender
    :param data: Data from the Database (in JSON format!)
    :param mapping: Mapping (a DSL for transforming JSON objects)
    :param json_schema: JSON Schema
    :return: Transformed Data
    """
    try:
        new_data = bend(mapping, data)
        validate_data(new_data, json_schema)
        return new_data
    except Exception as e:
        logging.error(err_msg[10] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that transform a GraphQL query into an SQL query
#def transform_graphql_to_sql(graphql_query, sql_schema_info, parameters, db_type):
    """
    Transform a GraphQL query into an SQL query
    :param graphql_query: GraphQL Query
    :param sql_schema_info: SQL Schema information
    :param parameters: Parameters (if any)
    :return: SQL Query
    """
    try:
        # Set the compiler metadata
        compiler_metadata = {
            "dialect": db_type,
        }
        # Transform the GraphQL query into an SQL query
        sql_query = graphql_to_sql(sql_schema_info, graphql_query, parameters, compiler_metadata)
        return sql_query
    except Exception as e:
        logging.error(err_msg[4] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

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
def etleng_sql_to_json(config_file, old_json_schema_file, json_schema_file, sql_query_file, json_file):
    """
    ETL Engine
    :param config_file: Configuration File
    :param old_json_schema_file: Old JSON Schema File
    :param json_schema_file: New JSON Schema File
    :param sql_query_file: SQL Query File
    :param json_file: JSON File
    :return: None
    """
    try:
        # Read the Configuration File
        config = read_config_file(config_file)
        # Read the Old JSON Schema
        old_json_schema = read_json_schema(old_json_schema_file)
        # Read the New JSON Schema
        json_schema = read_json_schema(json_schema_file)
        # Read the SQL Query
        sql_query = read_sql_query(sql_query_file)
        # Run the ETL Pipeline
        etleng_run_pipeline(config, old_json_schema, json_schema, sql_query, json_file)
    except Exception as e:
        logging.error(err_msg[13] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)

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
def etleng_run_pipeline(config, old_json_schema, json_schema, sql_query, json_file):
    """
    Run the ETL Pipeline
    :param config: Configuration
    :param old_json_schema: Old JSON Schema
    :param json_schema: New JSON Schema
    :param sql_query: SQL Query
    :param json_file: JSON File
    :return: None
    """
    try:
        # Read the Data from the Database
        data = read_data_from_db(config, sql_query)

        # Transform the Data from the old JSON schema to the new JSON schema
        if config['old_json_schema'] != '':
            data = transform_data_old_to_new(data, old_json_schema, json_schema)
            # Validate the Data
            data = validate_data(data.to_dict('records'), json_schema)
        else:
            data = etleng_df_to_json(data, json_schema)

        # Write the Data to a JSON file
        write_data_to_json(data, json_file)
    except Exception as e:
        logging.error(err_msg[14] + str(e))
        logging.error(err_msg[0].format(traceback.format_exc()))
        sys.exit(1)
