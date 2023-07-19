# Description and instructions:
# Some Unit Tests for the dbconn module.
# These tests will test the DBConn class methods and attributes using unittest methods
# and using functions get_db_connection. The get_db_connection function will be used to
# create a DBConn object to test the DBConn class methods and attributes.
# to use get_db_connection function, the config file must be in the same directory as the test_dbconn.py file
# and the config file must be named config.yml
# The config.yml file must be in the same directory as the test_dbconn.py file
# The config.yml file must be named config.yml
# The config.yml file must contain the following:
# host: <host>
# port: <port>
# database: <database>
# user: <user>
# password: <password>
# db_type: <db_type>
# The db_type must be one of the following: postgresql, mysql, or snowflake
# For snowflake, the config.yml file must contain the following:
# account: <account>
# warehouse: <warehouse>
# role: <role>
# schema: <schema>

# Import required modules:
import unittest
import os
import sys
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import datetime
import logging
import json
import yaml
import time
import traceback

# Import DBConn to test it:
import dbconn
from dbconn import interface as dbc

# Some utility functions:

# Read the Configuration File
def read_config_file(config_file):
    """
    Read the Configuration File
    :param config_file: Configuration File
    :return: Configuration
    """
    try:
        with open(config_file, 'r') as config:
            config = yaml.load(config, Loader=yaml.FullLoader)
            return config
    except Exception as e:
        logging.error("Error in reading the Configuration file: " + str(e))
        logging.error(traceback.format_exc())
        sys.exit(1)

# Load YAML test configuration file:
config_file = './tests/jobs/dbconn_test_config.yml'
config = read_config_file(config_file)

# Setup a generic SQL query to test the microetl.py module:
sql_query = """
                SELECT * FROM public.test_table;
            """

# Test class for the DBConn class
class TestDBConn(unittest.TestCase):

    # setUpClass method to create a DBConn object to test the DBConn class methods and attributes
    # This method will be called before any test methods are run
    # This method will create a DBConn object to test the DBConn class methods and attributes
    # This method will use the get_db_connection function to create a DBConn object
    # The get_db_connection function will use the config.yml file to create a DBConn object
    @classmethod
    def setUpClass(cls):
        cls.dbconn = dbc.get_db_connection(config)
        TestDBConn.dbconn = cls.dbconn
        
    # tearDownClass method to delete the DBConn object
    # This method will be called after all test methods are run
    # This method will delete the DBConn object
    @classmethod
    def tearDownClass(cls):
        dbc.close_db_connection(cls.dbconn, config.get('datasources').get('source').get('db_type'))
        del cls.dbconn

    # Test method to test the  dbconn object to find the db_type
    def test_DBC001_dbconn_db_type(self):
        db_type = config.get('datasources').get('source').get('db_type', '').lower().strip(' ')
        if db_type == "":
            self.assertEqual

    # Test is DB connection is not None
    def test_DBC002_dbconn_not_none(self):
        db_type = config.get('datasources').get('source').get('db_type')
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type != "none":
            self.assertIsNotNone(self.dbconn)
        else:
            # If db_type is none, then the DBConn object should be None
            self.assertIsNone(self.dbconn)

    # Test method to test the DBConn class attributes
    # This method will test the DBConn class attributes
    def test_DBC003_dbconn_check_attributes(self):
        db_type = config.get('datasources').get('source').get('db_type')
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')

        if db_type == 'snowflake':
            self.assertTrue(hasattr(self.dbconn, 'account'))
            self.assertTrue(hasattr(self.dbconn, 'warehouse'))
            self.assertTrue(hasattr(self.dbconn, 'role'))
            self.assertTrue(hasattr(self.dbconn, 'schema'))

        if db_type == 'postgres' or db_type == 'mysql' or db_type == 'postgresql':
            self.assertTrue(hasattr(self.dbconn, 'host'))
            self.assertTrue(hasattr(self.dbconn, 'port'))
            self.assertTrue(hasattr(self.dbconn, 'database'))
            self.assertTrue(hasattr(self.dbconn, 'user'))
            self.assertTrue(hasattr(self.dbconn, 'password'))
        else:
            # If db_type is none, then the DBConn class attributes should not be available
            pass

    # Test method to test the DBConn class methods
    # this test fundamentally ensure that the DBConn 
    # Public API is not modified to the point it would
    # break processes using it
    def test_DBC004_dbconn_check_methods(self):
        self.assertTrue(hasattr(dbc, 'get_db_connection'))
        self.assertTrue(callable(getattr(dbc, 'get_db_connection')))
        self.assertTrue(hasattr(dbc, 'get_db_cursor'))
        self.assertTrue(callable(getattr(dbc, 'get_db_cursor')))
        self.assertTrue(hasattr(dbc, 'close_db_connection'))
        self.assertTrue(callable(getattr(dbc, 'close_db_connection')))
        
    # Test method to test the DBConn class method execute_db_query
    def test_DBC005_check_execute_db_query(self):
        self.assertTrue(hasattr(dbc, 'execute_db_query'))
        self.assertTrue(callable(getattr(dbc, 'execute_db_query')))

    def test_DBC006_run_execute_db_query(self):
        db_type = config.get('datasources').get('source').get('db_type', '').lower().strip(' ')
        query = sql_query
        dbc.execute_db_query(self.dbconn, dbc.get_db_cursor(self.dbconn, db_type), query, db_type)

    # Test method to test the DBConn class method execute_db_query_return_results
    # This method will test the DBConn class method execute_db_query_return_results
    # and print the results
    def test_DBC006_check_execute_db_query_return_results(self):
        self.assertTrue(hasattr(dbc, 'execute_db_query_return_results'))
        self.assertTrue(callable(getattr(dbc, 'execute_db_query_return_results')))
    
    def test_DBC007_run_execute_db_query_return_results(self):
        query = sql_query
        results = dbc.execute_db_query_return_results(self.dbconn, dbc.get_db_cursor(self.dbconn, config.get('datasources').get("source").get('db_type')), query, config.get('datasources').get("source").get('db_type'))
        print(results)

    # Test method to test the DBConn class method execute_db_query_return_dataframe
    # This method will test the DBConn class method execute_db_query_return_dataframe
    # and print the results
    def test_DBC008_check_execute_db_query_return_dataframe(self):
        self.assertTrue(hasattr(dbc, 'execute_db_query_return_dataframe'))
        self.assertTrue(callable(getattr(dbc, 'execute_db_query_return_dataframe')))

    def test_DBC009_run_execute_db_query_return_dataframe(self):
        query = sql_query
        results = dbc.execute_db_query_return_dataframe(self.dbconn, dbc.get_db_cursor(self.dbconn, config.get('datasources').get("source").get('db_type')), query, config.get('datasources').get("source").get('db_type'))
        print(results)

    # Test method to test the DBConn class method execute_db_query_return_json
    # This method will test the DBConn class method execute_db_query_return_json
    # and print the results
    def test_DBC010_check_execute_db_query_return_json(self):
        self.assertTrue(hasattr(dbc, 'execute_db_query_return_json'))
        self.assertTrue(callable(getattr(dbc, 'execute_db_query_return_json')))

    def test_DBC011_run_execute_db_query_return_json(self):
        query = sql_query
        results = dbc.execute_db_query_return_json(self.dbconn, dbc.get_db_cursor(self.dbconn, config.get('datasources').get("source").get('db_type')), query, config.get('datasources').get("source").get('db_type'))
        print(results)
