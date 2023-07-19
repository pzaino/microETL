#############################################################
#    Name: DBConn (public interface)
# Release: 0.0.1
# Purpose: Minimal Database abstraction layer for ETL Engine
#  Author: Paolo Fabio Zaino 
#   Usage: Check docs/ETLEng.md
#############################################################

# Import Python Libraries
import os
import sys
import logging
import traceback
import datetime
import time
import re

# Import some data library modules
import boto3
import json
import pandas as pd
import numpy as np
import yaml

# Import Snowflake Connector
from dbconn import plugin_snowflake as sf

# Import Elasticsearch Connector
from dbconn import plugin_elasticsearch as es

# Import Neo4J Connector
from dbconn import plugin_neo4j as neo4j

# Import Postgres Connector
from dbconn import plugin_postgres as postgres

# Import MongoDB Connector
from dbconn import plugin_mongodb as mongodb

# Import MySQL Connector
#from dbconn import plugin_mysql

# Import error messages:
from dbconn import error_msg as erx

# function that returns a generic connection object to the database (using one of the available plugins)
# accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_db_connection(kwargs, target: str = 'source'):
    """
    Get Database Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: Database Connection Object
    """
    try:
        # Create a database connection object
        if kwargs.get('datasources').get(target).get('db_type') is None:
            db_type = 'none'
        else:
            db_type = kwargs.get('datasources').get(target).get('db_type')
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'postgres':
            return postgres.get_connection(kwargs)
        #elif db_type == 'mysql':
        #    return _get_mysql_connection(kwargs)
        elif db_type == 'neo4j':
            return neo4j.get_connection(kwargs)
        elif db_type == 'elasticsearch':
            return es.get_connection(kwargs)
        elif db_type == 'mongodb':
            return mongodb.get_connection(kwargs)
        elif db_type == 'snowflake':
            return sf.get_connection(kwargs)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function to close a database connection
def close_db_connection(conn, db_type):
    """
    Close Database Connection
    :param conn: Database Connection Object
    :param db_type: Database Type
    :return: None
    """
    try:
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'snowflake':
            sf.close_connection(conn)
        elif db_type == 'postgres':
            postgres.close_connection(conn)
        #elif db_type == 'mysql':
        #    mysql.close_connection(conn)
        elif db_type == 'neo4j':
            neo4j.close_connection(conn)
        elif db_type == 'elasticsearch':
            es.close_connection(conn)
        elif db_type == 'mongodb':
            mongodb.close_connection(conn)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that returns a generic cursor object to the database (using one of the available plugins)
def get_db_cursor(conn, db_type):
    """
    Get Database Cursor
    :param conn: Database Connection Object
    :param db_type: Database Type
    :return: Database Cursor Object
    """
    try:
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'snowflake':
            return sf.get_cursor(conn)
        elif db_type == 'postgres':
            return postgres.get_cursor(conn)
        #elif db_type == 'mysql':
        #    return _get_mysql_cursor(conn)
        elif db_type == 'neo4j':
            return neo4j.get_cursor(conn)
        elif db_type == 'elasticsearch':
            return es.get_cursor(conn)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the database (using one of the available plugins)
def execute_db_query(conn, cur, query, db_type):
    """
    Execute Database Query
    :param conn: Database Connection Object
    :param cur: Database Cursor Object
    :param query: Query to execute
    :param db_type: Database Type
    :return: None
    """
    try:
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'snowflake':
            sf.exec_query(conn, cur, query)
        elif db_type == 'postgres':
            postgres.exec_query(conn, cur, query)
        elif db_type == 'neo4j':
            neo4j.exec_query(conn, cur, query)
        #elif db_type == 'mysql':
        #    _execute_mysql_query(conn, cur, query)
        elif db_type == 'elasticsearch':
            es.exec_query(conn, cur, query)
        elif db_type == 'mongodb':
            mongodb.exec_query(conn, cur, query)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the database (using one of the available plugins) 
# and returns the results
def execute_db_query_return_results(conn, cur, query, db_type):
    """
    Execute Database Query and Return Results
    :param conn: Database Connection Object
    :param cur: Database Cursor Object
    :param query: Query to execute
    :param db_type: Database Type
    :return: Results
    """
    try:
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'snowflake':
            return sf.exec_query_return_results(conn, cur, query)
        elif db_type == 'postgres':
            return postgres.exec_query_return_results(conn, cur, query)
        #elif db_type == 'mysql':
        #    return _execute_mysql_query_return_results(conn, cur, query)
        elif db_type == 'elasticsearch':
            return es.exec_query_return_results(conn, cur, query)
        elif db_type == 'mongodb':
            return mongodb.exec_query_return_results(conn, cur, query)
        elif db_type == 'neo4j':
            return neo4j.exec_query_return_results(conn, cur, query)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the database (using one of the available plugins) 
# and returns the results as a dataframe
def execute_db_query_return_dataframe(conn, cur, query, db_type):
    """
    Execute Database Query and Return Dataframe
    :param conn: Database Connection Object
    :param cur: Database Cursor Object
    :param query: Query to execute
    :param db_type: Database Type
    :return: Dataframe
    """
    try:
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'snowflake':
            return sf.exec_query_return_dataframe(conn, cur, query)
        elif db_type == 'postgres':
            return postgres.exec_query_return_dataframe(conn, cur, query)
        #elif db_type == 'mysql':
        #    return _execute_mysql_query_return_dataframe(conn, cur, query)
        elif db_type == 'neo4j':
            return neo4j.exec_query_return_dataframe(conn, cur, query)
        elif db_type == 'mongodb':
            return mongodb.exec_query_return_dataframe(conn, cur, query)
        elif db_type == 'elasticsearch':
            return es.exec_query_return_dataframe(conn, cur, query)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function that runs a query and returns the results as a JSON object
def execute_db_query_return_json(conn, cur, query, db_type):
    """
    Execute Database Query and Return JSON
    :param conn: Database Connection Object
    :param cur: Database Cursor Object
    :param query: Query to execute
    :param db_type: Database Type
    :return: JSON
    """
    try:
        if db_type is None:
            db_type = 'none'
        db_type = str(db_type).lower().strip(' ')
        if db_type == 'none':
            return None
        elif db_type == 'snowflake':
            return sf.exec_query_return_json(conn, cur, query)
        elif db_type == 'postgres':
            return postgres.exec_query_return_json(conn, cur, query)
        #elif db_type == 'mysql':
        #    return _execute_mysql_query_return_json(conn, cur, query)
        elif db_type == 'elasticsearch':
            return es.exec_query_return_json(conn, cur, query)
        elif db_type == 'mongodb':
            return mongodb.exec_query_return_json(conn, cur, query)
        elif db_type == 'neo4j':
            return neo4j.exec_query_return_json(conn, cur, query)
        else:
            logging.error(erx.msg[1])
            sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
        