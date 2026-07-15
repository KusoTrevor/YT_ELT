import json
from datetime import date

import logging

logger = logging.getLogger(__name__)

def load_path():
    """
    Load data from a specified path into the data warehouse.
    This function reads data files from the given path and processes them
    for insertion into the data warehouse.

    Returns:
        None
    """
    # Implementation for loading data from the specified path
    file_path = f"./data/Yt_data_{date.today()}.json"  # Replace with the actual path to your data files

    try:
        logger.info(f"Loading data from {file_path} into the data warehouse.")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)

        return data  # Return the loaded data for further processing
    except filenotfoundError:
        logger.error(f"File not found: {file_path}. Please check the path and try again.")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file: {file_path}. Please check the file format.")
        raise