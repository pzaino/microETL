#####################################################################
# purpose: utility functions for database connection
#  author: Paolo Fabio Zaino
# release: 0.0.1
#  status: development
#####################################################################

# import required modules
import os
import sys
import logging
import traceback
import configparser

# Import error messages
from dbconn import error_msg as erx

# Function to strip username and password from dangerous characters
def strip_dangerous_characters(string, str_type: str = 'username'):
    """
    Strip Dangerous Characters
    :param string: String
    :return: String
    """
    try:
        str_type = str_type.lower().strip(' ')
        if str_type == 'username' or str_type == 'user' or str_type == 'password':
            return string.replace('\'', '').replace('"', '').replace(' ', '')
        else:
            return string.replace('\'', '').replace('"', '').strip(' ')
    except Exception as e:
        logging.error(erx.msg[0].format(str(e)))
        logging.error(erx.msg[0].format(traceback.format_exc()))
        return ''
