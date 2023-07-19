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

import pymongo
from pymongo import MongoClient
from pymongo import ReadPreference
from pymongo import WriteConcern
pm = pymongo.MongoClient()
import pymongo.errors

# Import error messages
from dbconn import error_msg as erx 

# Import utilities
from dbconn import utilities as utils

# Function that returns a connection object for MongoDB database
# and accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_connection(kwargs, target: str = 'source'):
    """
    Get MongoDB Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: MongoDB Connection Object
    """
    try:
        # Create a MongoDB connection object
        usr = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('user',''), 'username')
        if usr == None:
            usr = os.environ.get('MONGODB_USER', 'root')
        pwd = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('password'), 'password')
        if pwd == None:
            pwd = os.environ.get('MONGODB_PASSWORD')
        host = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('host', 'localhost'), 'host')
        port = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('port', 27017), 'port')
        database = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('database', ''), 'database')
        CONNECTION_STRING = "mongodb://" + str(usr) +":" + str(pwd) + "@" + str(host) + ":" + str(port) + "/" + str(database)
        conn = MongoClient(CONNECTION_STRING)
        return conn
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function to close a MongoDB connection
def close_connection(conn):
    """
    Close MongoDB Connection
    :param conn: MongoDB Connection Object
    :return: None
    """
    try:
        conn.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that returns a cursor object to the MongoDB database
def get_cursor(conn):
    """
    Get MongoDB Cursor
    :param conn: MongoDB Connection Object
    :return: MongoDB Cursor Object
    """
    try:
        # Create a MongoDB cursor object
        cur = conn.cursor()
        return cur
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the MongoDB database
def exec_query(conn, cur, query):
    """
    Execute MongoDB Query
    :param conn: MongoDB Connection Object
    :param cur: MongoDB Cursor Object
    :param query: Query to execute
    :return: None
    """
    try:
        # Execute the query
        cur.execute(query)
        # Commit the changes
        conn.commit()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the mongodb database and returns the results
def exec_query_return_results(conn, cur, query):
    """
    Execute MongoDB Query and Return Results
    :param conn: MongoDB Connection Object
    :param cur: MongoDB Cursor Object
    :param query: Query to execute
    :return: Results
    """
    try:
        # Execute the query
        cur.execute(query)
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

# function that executes a query on the mongodb database and returns the results as a dataframe
def exec_query_return_dataframe(conn, cur, query):
    """
    Execute MongoDB Query and Return Dataframe
    :param conn: MongoDB Connection Object
    :param cur: MongoDB Cursor Object
    :param query: Query to execute
    :return: Dataframe
    """
    try:
        # Execute the query
        cur.execute(query)
        conn.commit()
        # Fetch the results
        results = cur.fetchall()
        # Convert the results to a dataframe
        df = pd.DataFrame(results)
        return df
    except pymongo.errors.ConnectionFailure as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except pymongo.errors.OperationFailure as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the mongodb database and returns a JSON object
def exec_query_return_json(conn, cur, query):
    """
    Execute MongoDB Query and Return JSON
    :param conn: MongoDB Connection Object
    :param cur: MongoDB Cursor Object
    :param query: Query to execute
    :return: JSON
    """
    try:
        # Execute the query
        cur.execute(query)
        conn.commit()
        # Fetch the results
        results = cur.fetchall()
        # Convert the results to a JSON object
        json_results = json.dumps(results)
        return json_results
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
