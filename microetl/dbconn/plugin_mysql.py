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

import MySQLdb as mysql
from MySQLdb.connections import Error

# Import error messages
from . import error_msg as erx

# Import utilities
from . import utilities as utils

# Function that returns a connection object for MySQL database
# and accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_connection(kwargs, target: str = 'source'):
    """
    Get MySQL Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: MySQL Connection Object
    """
    try:
        # Create a MySQL connection object
        usr = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('user', ''), 'username')
        if usr == '':
            usr = os.environ.get('MYSQL_USER')
        pwd = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('password', ''), 'password')
        if pwd == '':
            pwd = os.environ.get('MYSQL_PASSWORD')
        conn = mysql.connect(
            host=str(kwargs.get('datasources').get(target).get('host', 'localhost')),
            port=str(kwargs.get('datasources').get(target).get('port', 3306)),
            user=usr,
            password=pwd,
            database=str(kwargs.get('datasources').get(target).get('database', ''))
        )
        return conn
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function to close a MySQL connection
def close_connection(conn):
    """
    Close MySQL Connection
    :param conn: MySQL Connection Object
    :return: None
    """
    try:
        conn.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that returns a cursor object to the MySQL database
def get_cursor(conn):
    """
    Get MySQL Cursor
    :param conn: MySQL Connection Object
    :return: MySQL Cursor Object
    """
    try:
        # Create a MySQL cursor object
        cur = conn.cursor(cursor_factory=mysql.Connect.cursor.MySQLCursorDict)
        return cur
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function to close a MySQL cursor
def close_cursor(cur):
    """
    Close MySQL Cursor
    :param cur: MySQL Cursor Object
    :return: None
    """
    try:
        cur.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the mysql database
def exec_query(conn, cur, query, query_params = None):
    """
    Execute MySQL Query
    :param conn: MySQL Connection Object
    :param cur: MySQL Cursor Object
    :param query: Query to execute
    :return: None
    """
    try:
        # Execute the query
        cur.execute(query, query_params)
        conn.commit()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# function that executes a query on the mysql database and returns the results
def exec_query_return_results(conn, cur, query, query_params = None):
    """
    Execute MySQL Query and Return Results
    :param conn: MySQL Connection Object
    :param cur: MySQL Cursor Object
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
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# function that executes a query on the mysql database and returns the results as a dataframe
def exec_query_return_dataframe(conn, cur, query, query_params = None):
    """
    Execute MySQL Query and Return Dataframe
    :param conn: MySQL Connection Object
    :param cur: MySQL Cursor Object
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
    except mysql.Connect.Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# Function that executes a Postgres query and returns the results as a JSON object
def exec_query_return_json(conn, cur, query, query_params=None):
    """
    Execute Postgres Query and Return JSON Object
    :param conn: Postgres Connection Object
    :param cur: Postgres Cursor Object
    :param query: Query to execute
    :param query_params: Query Parameters
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
        json_data = results.to_json(orient = 'records')
        return json_data
    except mysql.Connect.Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)