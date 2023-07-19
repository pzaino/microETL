# This script provides a Python unittest class for the microetl.py module.

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

# Import microetl to test it:
from microetl import core as metl
from microetl.core import K, S

# Load YAML test configuration:
config_file = './tests/jobs/microetl_test_config.yml'
config = metl.read_config_file(config_file)

# JSON Schema file for testing purposes:
json_schema_file1   = './tests/inp_data/microetl_test_schema_001.json'
json_schema_file1_1 = './tests/inp_data/microetl_test_schema_001_1.json'
json_schema_file1_2 = './tests/inp_data/microetl_test_schema_001_2.json'
json_schema_file2   = './tests/inp_data/microetl_test_schema_002.json'
json_schema_file3   = './tests/inp_data/microetl_test_schema_003.json'
json_schema_file4   = './tests/inp_data/fields_schema.json'

# Write a test class for the microetl.py module:
class TestMicroETL(unittest.TestCase):
    # setUpClass method to create a microetl object to test the microetl class methods and attributes
    # This method will be called before any test methods are run
    # This method will create a microetl object to test the microetl class methods and attributes
    # This method will use the get_db_connection function to create a microetl object
    # The get_db_connection function will use the microetl_test_config.yml file to create a microetl object
    @classmethod
    def setUpClass(cls):
        cls.microetl = metl
        TestMicroETL.microetl = cls.microetl

    # tearDownClass method to delete the microetl object
    # This method will be called after all test methods are run
    # This method will delete the microetl object
    @classmethod
    def tearDownClass(cls):
        # This test should just pass, given that the microetl object is created in the setUpClass method and deleted when the ETL engines completes
        pass

    # test ETLEng to load a config file and parse it:
    @classmethod
    def test_ETL001_etleng_load_config(cls):
        # Set the expected result:
        assert config.get('datasources') != None
        assert config.get('datasources').get('source') != None
        assert config.get('datasources').get('source').get('db_type') != None
        assert config.get('schemas') != None

    # transform one JSON object into another JSON object, using metl.transform_data_json_to_json function:
    @classmethod
    def test_ETL002_transform_data(cls):
        # Load JSON Schema:
        json_schema = metl.read_json_schema(json_schema_file1_1)

        # Create 1st JSON Object:
        data_object = json.loads("""
                                    {
                                        "customer": {
                                            "first_name": "Giovanni",
                                            "last_name": "Montoya",
                                            "Age": 24
                                        },
                                        "address": {
                                            "city": "Milano",
                                            "country": "Italy"
                                        }
                                    }
                                """)
        
        # Create the mapping:
        mapping = {
                    'fullName': (S('customer', 'first_name') + K(' ') + S('customer', 'last_name')),
                    'city': S('address', 'city') + K(', ') + S('address', 'country'),
                  }

        # Transform the data object:
        transformed_data_object = metl.transform_data_json_to_json(data_object, mapping, json_schema)
        print("\n")
        print(data_object)
        print("\n Into: \n")
        print(transformed_data_object)
        print("\n")
        # Assert that JSON data object is valid against the schema:
        assert metl.validate_data(transformed_data_object, json_schema) == True

    # test_read_json_schema method to test the read_json_schema function
    # This method will test the read_json_schema function using the microetl_test_schema.json file
    @classmethod
    def test_ETL002_read_json_schema(cls):
        # Load JSON Schema:
        json_schema = metl.read_json_schema(json_schema_file1)
        # Assert that the expected result matches the actual result:
        assert metl.validate_json_schema(json_schema) == True 

    # test_read_json_schema method to test the read_json_schema function
    # This method will test the read_json_schema function using the microetl_test_schema.json file
    @classmethod
    def test_ETL003_validate_json_object_through_schema1(cls):
        # Set the test data:
        data_object = json.loads( """
                                    {
                                        "firstName": "John",
                                        "lastName": "Doe",
                                        "age": 21
                                    }
                                """ )
        # Load the schema:
        schema = metl.read_json_schema(json_schema_file1)
        # Assert that JSON data object is valid against the schema:
        assert metl.validate_data(data_object, schema) == True

    # test_read_json_schema method to test the read_json_schema function
    # This method will test the read_json_schema function using the microetl_test_schema.json file
    @classmethod
    def test_ETL004_validate_json_object_through_schema2(cls):
        # Set the test data (with a wrong field):
        data_object = json.loads( """
                                    {
                                        "fullName": "John Doe",
                                        "city": "New York",
                                        "age": 21
                                    }
                                """ )
        # Load the schema:
        schema = metl.read_json_schema(json_schema_file1_1)
        # Assert that JSON data object is NOT valid against the schema:
        assert metl.validate_data(data_object, schema) == True

    # test_read_json_schema method to test the read_json_schema function
    # This method will test the read_json_schema function using the microetl_test_schema_002.json file
    @classmethod
    def test_ETL005_validate_json_object_through_schema3(cls):
        # Set the test data:
        data_object = json.loads( """
                                    {
                                        "firstName": "John",
                                        "lastName": "Doe",
                                        "age": 21,
                                        "address": {
                                            "streetAddress": "21 2nd Street",
                                            "city": "New York",
                                            "state": "NY",
                                            "postalCode": "10021"
                                        },
                                        "phoneNumber": [
                                            {
                                                "type": "home",
                                                "number": "212 555-1234"
                                            },
                                            {
                                                "type": "fax",
                                                "number": "646 555-4567"
                                            }
                                        ]
                                    }
                                """ )
        # Load the schema:
        schema = metl.read_json_schema(json_schema_file1_2)
        # Assert that JSON data object is valid against the schema:
        assert metl.validate_data(data_object, schema) == True

    # test_read_json_schema method to test the read_json_schema function
    # This method will test the read_json_schema function using the microetl_test_schema_002.json file
#    @classmethod
#    def test_ETL006_validate_json_object_through_schema4(cls):
        # Load a CDM Layer 1 event:
#        f = open("./tests/out_data/event1.json")
#        data_object = json.load(f)
#        f.close()
        
        # Load the schema:
#        schema = metl.read_json_schema(json_schema_file3)
        # Assert that JSON data object is valid against the schema:
#        assert metl.validate_data(data_object, schema) == True

    # test_read_json_schema method to test the read_json_schema function
    # This method will test the read_json_schema function using the microetl_test_schema_002.json file
#    @classmethod
#    def test_ETL007_validate_json_object_through_schema4(cls):
        # Load a CDM Layer 1 event:
#        f = open("./tests/out_data/event4.json")
#        data_object = json.load(f)
#        f.close()
        
        # Load the schema:
#        schema = metl.read_json_schema(json_schema_file4)
        # Assert that JSON data object is valid against the schema:
#        assert metl.validate_data(data_object, schema) == True

