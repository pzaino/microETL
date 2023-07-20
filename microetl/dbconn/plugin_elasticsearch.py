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

import elasticsearch
import elasticsearch.helpers

# Import error messages
from . import error_msg as erx

# Import utilities
from . import utilities as utils

# Function that returns a connection object for ElasticSearch database
# and accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_connection(kwargs, target: str = 'source'):
    """
    Get ElasticSearch Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: ElasticSearch Connection Object
    """
    try:
        # Create a ElasticSearch connection object
        pwd = kwargs.get("datasources").get(target).get('password')
        if pwd == None:
            pwd = str(os.environ.get('ELASTIC_PASSWORD'))
        if kwargs.get("datasources").get(target).get('cloud_id') != None:
            conn = elasticsearch.Elasticsearch( 
                hosts=str(kwargs.get("datasources").get(target).get('hosts', '')),
                basic_auth=("elastic", pwd),
                ca_certs=str(kwargs.get("datasources").get(target).get('ca_certs', '')),
            )
        else:
            conn = elasticsearch.Elasticsearch( 
                cloud_id=kwargs.get("datasources").get(target).get('cloud_id'),
                basic_auth=("elastic", pwd),
                ca_certs=kwargs.get("datasources").get(target).get('ca_certs', 1),
            )
        return conn
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function to close a ElasticSearch connection
def close_connection(conn):
    """
    Close ElasticSearch Connection
    :param conn: ElasticSearch Connection Object
    :return: None
    """
    try:
        conn.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that returns a generic cursor object to the database using Elasticsearch
def get_cursor(conn):
    """
    Get Elasticsearch Cursor
    :param conn: Elasticsearch Connection Object
    :return: Elasticsearch Cursor Object
    """
    try:
        # Create a Elasticsearch cursor object
        cur = conn.cursor()
        return cur
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function to close a Elasticsearch cursor
def close_cursor(cur):
    """
    Close Elasticsearch Cursor
    :param cur: Elasticsearch Cursor Object
    :return: None
    """
    try:
        cur.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the Elasticsearch database
def exec_query(conn, cur, query):
    """
    Execute Elasticsearch Query
    :param conn: Elasticsearch Connection Object
    :param cur: Elasticsearch Cursor Object
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

# function that executes a query on the Elasticsearch database and returns the results
def exec_query_return_results(conn, cur, query):
    """
    Execute Elasticsearch Query and Return Results
    :param conn: Elasticsearch Connection Object
    :param cur: Elasticsearch Cursor Object
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

# function that executes a query on the elasticsearch database and returns the results as a dataframe
def exec_query_return_dataframe(conn, cur, query):
    """
    Execute Elasticsearch Query and Return Dataframe
    :param conn: Elasticsearch Connection Object
    :param cur: Elasticsearch Cursor Object
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
    except elasticsearch.exceptions.ConnectionError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except elasticsearch.exceptions.RequestError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except elasticsearch.exceptions.TransportError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except elasticsearch.exceptions.ApiError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# Function that executes an Elasticsearch query and returns the results as a JSON object
def exec_query_return_json(conn, cur, query):
    """
    Execute Elasticsearch Query and Return JSON
    :param conn: Elasticsearch Connection Object
    :param cur: Elasticsearch Cursor Object
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
    except elasticsearch.exceptions.ConnectionError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except elasticsearch.exceptions.RequestError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except elasticsearch.exceptions.TransportError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except elasticsearch.exceptions.ApiError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)
