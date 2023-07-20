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

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extensions import ISOLATION_LEVEL_DEFAULT
from psycopg2.extensions import TRANSACTION_STATUS_IDLE
from psycopg2.extensions import TRANSACTION_STATUS_ACTIVE
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS
from psycopg2.extensions import TRANSACTION_STATUS_INERROR
from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN
from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN
from psycopg2.extensions import TRANSACTION_STATUS_IDLE
from psycopg2.extensions import TRANSACTION_STATUS_ACTIVE
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS
from psycopg2.extensions import TRANSACTION_STATUS_INERROR
from psycopg2.extensions import TRANSACTION_STATUS_UNKNOWN

# Import error messages
from . import error_msg as erx

# Import utilities
from . import utilities as utils

# function that returns a connection object to the postgres database
# and accept db connection parameters as a collection of keyword arguments
# passed to the function
def get_connection(kwargs, target: str = 'source'):
    """
    Get Postgres Connection
    :param kwargs: Keyword Arguments
    :param target: Target (source or destination)
    :return: Postgres Connection Object
    """
    try:
        # Create a Postgres connection object
        usr = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('user', ''), 'username')
        if usr == '':
            usr = os.environ.get('POSTGRES_USER', 'postgres')
        pwd = utils.strip_dangerous_characters(kwargs.get("datasources").get(target).get('password', ''), 'password')
        if pwd == '':
            pwd = os.environ.get('POSTGRES_PASSWORD', 'postgres')

        conn = psycopg2.connect(
            host=str(kwargs.get("datasources").get(target).get('host', 'localhost')),
            port=str(kwargs.get("datasources").get(target).get('port', 5432)),
            user=usr,
            password=pwd,
            database=str(kwargs.get("datasources").get(target).get('database', ''))
        )
        return conn
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function to close a postgres connection
def close_connection(conn):
    """
    Close Postgres Connection
    :param conn: Postgres Connection Object
    :return: None
    """
    try:
        # Close the Postgres connection
        conn.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# Function that closes a postgres cursor
def close_cursor(cur):
    """
    Close Postgres Cursor
    :param cur: Postgres Cursor Object
    :return: None
    """
    try:
        # Close the Postgres cursor
        cur.close()
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        sys.exit(1)

# function that returns a cursor object to the postgres database
def get_cursor(conn):
    """
    Get Postgres Cursor
    :param conn: Postgres Connection Object
    :return: Postgres Cursor Object
    """
    try:
        # Create a Postgres cursor object
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cur
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        sys.exit(1)

# function that executes a query on the postgres database
def exec_query(conn, cur, query):
    """
    Execute Postgres Query
    :param conn: Postgres Connection Object
    :param cur: Postgres Cursor Object
    :param query: Query to execute
    :return: None
    """
    try:
        # Execute the query
        cur.execute(query)
        conn.commit()
    except psycopg2.OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# function that executes a query on the postgres database and returns the results
def exec_query_return_results(conn, cur, query):
    """
    Execute Postgres Query and Return Results
    :param conn: Postgres Connection Object
    :param cur: Postgres Cursor Object
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
    except psycopg2.OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# function that executes a query on the postgres database and returns the results as a dataframe
def exec_query_return_dataframe(conn, cur, query):
    """
    Execute Postgres Query and Return Dataframe
    :param conn: Postgres Connection Object
    :param cur: Postgres Cursor Object
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
    except psycopg2.OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)
    except psycopg2.Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        sys.exit(1)

# Function that executes a Postgres query and returns the results as a JSON object
def exec_query_return_json(conn, cur, query):
    """
    Execute Postgres Query and Return JSON Object
    :param conn: Postgres Connection Object
    :param cur: Postgres Cursor Object
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
        json_data = json.dumps(results)
        return json_data
    except psycopg2.OperationalError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        return None
    except psycopg2.ProgrammingError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        return None
    except psycopg2.DatabaseError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        return None
    except psycopg2.InterfaceError as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        return None
    except psycopg2.Error as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format
                        (traceback.format_exc()))
        conn.rollback()
        return None
