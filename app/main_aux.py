import argparse
import json
import sys
import logging
from datetime import datetime


def create_logger(log_level=logging.INFO):
    """
    Function to create a logger object for use in the script
    :param object log_level: logger.[LEVEL] object to set the default level of the logs to display
    :return object logger: Standard python logging object
    """
    logger = logging.getLogger("elmsMetadataDQTool")
    formatter = logging.Formatter(f"[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s")

    handlers = [logging.StreamHandler(), logging.FileHandler(f"logs/elmsMetadata-log-{datetime.now()}")]
    for handler in handlers:
        handler.setLevel(log_level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(log_level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def parse_args():
    """
    Argument parser function to ensure users input the mode of the script and the path to the config file.
    :return object args: ArgParser Namespace object that contains key value pairs accessible by attribute names where
    values are the values provided by the user as a string
    """
    arg_parser = argparse.ArgumentParser(description="A command line tool to run metadata creation and "
                                                     "data quality analysis on ELMS data held in cloud storage.",
                                         usage='%(prog)s [options]')
    arg_parser.add_argument('mode',
                            type=str,
                            help='choose between "dq" for data quality, or "metadata" for metadata generation')
    arg_parser.add_argument('config',
                            type=str,
                            help='full path to the config file')
    args = arg_parser.parse_args()
    _validate_args(args)
    return args


def _validate_args(args):
    """
    Light function to ensure the mode given by the user conforms to either of 'dq' or 'metadata' to ensure conformity
    for the rest of the script. Prints error and exits script if conformity not met.
    :param object args: ArgParser Namespace object
    """
    if args.mode not in ['dq', 'metadata']:
        print("Improper mode given as argument, please choose from:\n- dq\n- metadata\n\nExiting.")
        sys.exit()


def load_json_file(file_path):
    """
    Function to upload JSON files into dictionaries given the file path.
    :param string file_path: File path string to json file
    :return dict file: Dict representation of the file
    """
    with open(file_path, 'r') as f:
        file = json.load(f)
    return file


def validate_config_file(config):
    pass
