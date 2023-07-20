#####################################################################
# purpose: database connection plugin for dbconn
#  author: Paolo Fabio Zaino
# release: 0.0.1
#  status: development
#####################################################################

# Import the required libraries
import os
import sys
import logging
import traceback
import pandas as pd
import json

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector import ProgrammingError
from snowflake.connector import OperationalError
from snowflake.connector import DatabaseError
from snowflake.connector import InterfaceError
from snowflake.connector import Error

# Import error messages
from . import error_msg as erx

# Import utilities
from . import utilities as utils

# Global Variables
# Snowflake Connection Variables
SNOWFLAKE_ACCOUNT = 'xxxxxx'
SNOWFLAKE_USER = 'xxxxxx'
SNOWFLAKE_DATABASE = 'xxxxxx'
SNOWFLAKE_SCHEMA = 'xxxxxx'
SNOWFLAKE_WAREHOUSE = 'xxxxxx'
SNOWFLAKE_ROLE = 'xxxxxx'
SNOWFLAKE_REGION = 'xxxxxx'
SNOWFLAKE_STAGE = 'xxxxxx'
SNOWFLAKE_FILE_FORMAT = 'xxxxxx'
SNOWFLAKE_PRIVATE_KEY = 'xxxxxx'
SNOWFLAKE_AUTHENTICATOR = 'xxxxxx'
SNOWFLAKE_TOKEN = 'xxxxxx'
SNOWFLAKE_LOGIN_TIMEOUT = 'xxxxxx'
SNOWFLAKE_NETWORK_TIMEOUT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_STATEMENTS = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_TIMEOUT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_RETRY_TIMEOUT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_QUEUE_TIMEOUT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_QUEUE_TIMEOUT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_RETRY_TIMEOUT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_QUEUE_SIZE = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_ERROR_COUNT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_ERROR_RATIO = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_TIMEOUT_COUNT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_TIMEOUT_RATIO = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_QUEUE_TIMEOUT_COUNT = 'xxxxxx'
SNOWFLAKE_CONCURRENT_TRANSACTIONS_MAX_QUEUE_TIMEOUT_RATIO = 'xxxxxx'

# function that returns a connection object to the snowflake database
# and accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_connection(kwargs, target: str = 'source'):
    """
    Get Snowflake Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: Snowflake Connection Object
    """
    try:
        # Create a Snowflake connection object
        usr = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('user', ''), 'username')
        if usr == '' or usr == None:
            usr = os.environ.get('SNOWFLAKE_USER', 'snowflake_user')
        pwd = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('password', ''), 'password')
        if pwd == '' or pwd == None:
            pwd = os.environ.get('SNOWFLAKE_PASSWORD')      
        conn = snowflake.connector.connect(
            user=usr,
            password=pwd,
            account=str(kwargs.get("datasources").get(target).get('account', 'xxxxxx')),
            database=str(kwargs.get("datasources").get(target).get('database', 'xxxxxx')),
            schema=str(kwargs.get("datasources").get(target).get('schema', 'public' )),
            warehouse=str(kwargs.get("datasources").get(target).get('warehouse', 'xxxxxx')),
            role=str(kwargs.get("datasources").get(target).get('role', 'xxxxxx')),
            region=str(kwargs.get("datasources").get(target).get('region')),
            stage=str(kwargs.get("datasources").get(target).get('stage')),
            file_format=str(kwargs.get("datasources").get(target).get('file_format')),
            private_key=str(kwargs.get("datasources").get(target).get('private_key')),
            private_key_passphrase=str(kwargs.get("datasources").get(target).get('private_key_passphrase')),
            authenticator=str(kwargs.get("datasources").get(target).get('authenticator')),
            token=str(kwargs.get("datasources").get(target).get('token')),
            login_timeout=str(kwargs.get("datasources").get(target).get('login_timeout')),
            network_timeout=str(kwargs.get("datasources").get(target).get('network_timeout')),
            concurrent_statements=str(kwargs.get("datasources").get(target).get('concurrent_statements')),
            concurrent_transactions=str(kwargs.get("datasources").get(target).get('concurrent_transactions')),
            concurrent_transactions_timeout=str(kwargs.get("datasources").get(target).get('concurrent_transactions_timeout')),
            concurrent_transactions_retry_timeout=str(kwargs.get("datasources").get(target).get('concurrent_transactions_retry_timeout')),
            concurrent_transactions_queue_timeout=str(kwargs.get("datasources").get(target).get('concurrent_transactions_queue_timeout')),
            concurrent_transactions_max_queue_timeout=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_queue_timeout')),
            concurrent_transactions_max_retry_timeout=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_retry_timeout')),
            concurrent_transactions_max_queue_size=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_queue_size')),
            concurrent_transactions_max_error_count=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_error_count')),
            concurrent_transactions_max_error_ratio=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_error_ratio')),
            concurrent_transactions_max_timeout_count=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_timeout_count')),
            concurrent_transactions_max_timeout_ratio=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_timeout_ratio')),
            concurrent_transactions_max_queue_timeout_count=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_queue_timeout_count')),
            concurrent_transactions_max_queue_timeout_ratio=str(kwargs.get("datasources").get(target).get('concurrent_transactions_max_queue_timeout_ratio'))
        )
        return conn
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that closes the connection to the snowflake database
def close_connection(conn):
    """
    Close Snowflake Connection
    :param conn: Snowflake Connection Object
    """
    try:
        # Close the Snowflake connection object
        conn.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that returns a cursor object to the snowflake database
def get_cursor(conn):
    """
    Get Snowflake Cursor
    :param conn: Snowflake Connection Object
    :return: Snowflake Cursor Object
    """
    try:
        # Create a Snowflake cursor object
        cur = conn.cursor(DictCursor)
        return cur
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function that closes the cursor object to the snowflake database
def close_cursor(cur):
    """
    Close Snowflake Cursor
    :param cur: Snowflake Cursor Object
    """
    try:
        # Close the Snowflake cursor object
        cur.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the snowflake database
def exec_query(conn, cur, query, query_params=None):
    """
    Execute Snowflake Query
    :param conn: Snowflake Connection Object
    :param cur: Snowflake Cursor Object
    :param query: Query to execute
    """
    try:
        # Execute the query
        cur.execute(query, query_params)
        conn.commit()
    except OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# function that executes a query on the snowflake database and returns the results
def exec_query_return_results(conn, cur, query, query_params=None):
    """
    Execute Snowflake Query and Return Results
    :param conn: Snowflake Connection Object
    :param cur: Snowflake Cursor Object
    :param query: Query to execute
    :return: Results
    """
    try:
        # Execute the query
        cur.execute(query, query_params)
        conn.commit()
        # Fetch the results
        results = cur.fetchall()
        return results
    except OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# function that executes a query on the snowflake database and returns the results as a dataframe
def exec_query_return_dataframe(conn, cur, query, query_params=None):
    """
    Execute Snowflake Query and Return Dataframe
    :param conn: Snowflake Connection Object
    :param cur: Snowflake Cursor Object
    :param query: Query to execute
    :return: Dataframe
    """
    try:
        # Execute the query
        cur.execute(query, query_params)
        conn.commit()
        # Fetch the results
        results = pd.DataFrame(cur.fetchall())
        results.columns=[ x.name for x in cur.description ]
        return results
    except OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# Function that executes a Snowflake query and returns the results as a JSON object
def exec_query_return_json(conn, cur, query, query_params=None):
    """
    Execute Snowflake Query and Return JSON
    :param conn: Snowflake Connection Object
    :param cur: Snowflake Cursor Object
    :param query: Query to execute
    :return: JSON Object
    """
    try:
        # Execute the query
        cur.execute(query, query_params)
        conn.commit()
        # Fetch the results
        results = pd.DataFrame(cur.fetchall())
        results.columns=[ x.name for x in cur.description ]
        # Convert the results to a JSON object
        json_results = results.to_json(orient = 'records')
        return json_results
    except OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
