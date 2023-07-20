#

# Import required modules:
import requests
import json
import yaml
import time
import logging
import datetime
import traceback

# Function to setup an API request header
# it takes a config object as input and returns a header object
def setup_api_header(config):
    """
    Setup an API request header
    :param config: the config object
    :return: the header object
    """

    # Set the default return value:
    header = None

    # Prepare the API request header:
    header = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + config.get('api').get('token')
    }

    # Return the header object:
    return header

# Function to setup an API request parameter which takes a config object as input and returns a parameter object
def setup_api_parameter(config):
    """
    Setup an API request parameter
    :param config: the config object
    :return: the parameter object
    """

    # Set the default return value:
    parameter = None

    # Prepare the API request parameter:
    parameter = {
        'limit': config.get('api').get('limit'),
        'offset': config.get('api').get('offset')
    }

    # Return the parameter object:
    return parameter

# Function to connect to an API and return the data
# it takes a config object as input and returns a data object
def get_data(url, headers, params):
    """
    Connect to an API and return the data
    :param request: the request object
    :param url: the url object
    :param headers: the headers object
    :param params: the params object
    :return: the data object
    """

    # Set the default return value:
    data = None

    # Connect to the API and get the data:
    response = requests.get(url, headers=headers, params=params)

    # Check the response status code:
    if response.status_code == 200:
        # Get the data:
        data = response.json()
    else:
        # Log the error:
        logging.error('Error: ' + str(response.status_code) + ' - ' + response.text)

    # Return the data object:
    return data

# Function to post a request to an API and return the data
# it takes header, url, and data objects as input and returns a data object
def post_data(request, url, headers):
    """
    Post a request to an API and return the data
    :param request: the request object
    :param url: the url object
    :param headers: the headers object
    :param data: the data object
    :return: the data object
    """

    # Set the default return value:
    data = None

    # Post the request to the API and get the data:
    response = requests.post(url, headers=headers, data=request)

    # Check the response status code:
    if response.status_code == 200:
        # Get the data:
        data = response.json()
    else:
        # Log the error:
        logging.error('Error: ' + str(response.status_code) + ' - ' + response.text)

    # Return the data object:
    return data

# Function that makes a soap request to an API and returns the data
# it takes a config object as input and returns a data object
def get_soap_data(config):
    """
    Make a soap request to an API and return the data
    :param config: the config object
    :return: the data object
    """

    # Set the default return value:
    data = None

    # Prepare the API request header:
    header = setup_api_header(config)

    # Prepare the API request parameter:
    parameter = setup_api_parameter(config)

    # Connect to the API and get the data:
    data = get_data(config.get('api').get('url'), header, parameter)

    # Return the data object:
    return data





