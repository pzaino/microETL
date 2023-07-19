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

import neo4j
import neo4j.exceptions

# Import error messages
from dbconn import error_msg as erx

# Import utilities
from dbconn import utilities as utils

# Function that returns a connection object for Neo4J database
# and accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_connection(kwargs, target: str = 'source'):
    """
    Get Neo4J Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: Neo4J Connection Object
    """
    try:
        # Create a Neo4J connection object
        usr = kwargs.get("datasources").get(target).get('user')
        if usr == None:
            usr = os.environ.get('NEO4J_USER', 'neo4j')
        pwd = kwargs.get("datasources").get(target).get('password')
        if pwd == None:
            pwd = os.environ.get('NEO4j_PASSWORD', 'neo4j')
        conn = neo4j.connect(
            host=str(kwargs.get("datasources").get(target).get('host', 'localhost')),
            port=str(kwargs.get("datasources").get(target).get('port', 7687)),
            user=usr,
            password=pwd,
            database=str(kwargs.get("datasources").get(target).get('database', 'neo4j'))
        )
        return conn
    except neo4j.exceptions.ServiceUnavailable as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)
    except neo4j.exceptions.AuthError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function to close a Neo4J connection
def close_connection(conn):
    """
    Close Neo4J Connection
    :param conn: Neo4J Connection Object
    :return: None
    """
    try:
        conn.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that returns a cursor object to the Neo4J database
def get_cursor(conn):
    """
    Get Neo4J Cursor
    :param conn: Neo4J Connection Object
    :return: Neo4J Cursor Object
    """
    try:
        # Create a Neo4J cursor object
        cur = conn.cursor(cursor_factory=neo4j.cursor.DictCursor)
        return cur
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the neo4j database
def exec_query(conn, cur, query):
    """
    Execute Neo4J Query
    :param conn: Neo4J Connection Object
    :param cur: Neo4J Cursor Object
    :param query: Query to execute
    :return: None
    """
    try:
        # Execute the query
        cur.execute(query)
        conn.commit()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback
        sys.exit(1)

# function that executes a query on the neo4j database and returns the results
def exec_query_return_results(conn, cur, query):
    """
    Execute Neo4j Query and Return Results
    :param conn: Neo4j Connection Object
    :param cur: Neo4j Cursor Object
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

# function that executes a query on the neo4j database and returns the results as a dataframe
def exec_query_return_dataframe(conn, cur, query):
    """
    Execute Neo4j Query and Return Dataframe
    :param conn: Neo4j Connection Object
    :param cur: Neo4j Cursor Object
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
    except neo4j.exceptions.ClientError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function that executes a neo4j query and returns the results as a JSON object
def exec_query_return_json(conn, cur, query):
    """
    Execute Neo4j Query and Return JSON
    :param conn: Neo4j Connection Object
    :param cur: Neo4j Cursor Object
    :param query: Query to execute
    :return: JSON Object
    """
    try:
        # Execute the query
        cur.execute(query)
        conn.commit()
        # Fetch the results
        results = cur.fetchall()
        # Convert the results to a JSON object
        json_data = []
        for result in results:
            json_data.append(dict(result))
        return json_data
    except neo4j.exceptions.ClientError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)
