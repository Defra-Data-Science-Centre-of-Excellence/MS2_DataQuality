import argparse
import json
import sys
import logging
from datetime import datetime
import os


def create_logger(log_level=logging.INFO):
    """
    Function to create a logger object for use in the script
    :param object log_level: logger.[LEVEL] object to set the default level of the logs to display
    :return object logger: Standard python logging object
    """
    if not os.path.exists("./logs"):
        os.mkdir("./logs")

    logger = logging.getLogger("elmsMetadataDQTool")
    formatter = logging.Formatter(f"[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s")
    # Stream handler to print log statements to command line when running script
    sh = logging.StreamHandler()
    sh.setLevel(log_level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    # File handler to write logs down to DEBUG level to local file on machine that runs the script
    fh = logging.FileHandler(f"logs/elmsMetadata-log-{datetime.now()}")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.setLevel(logging.DEBUG)
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


def write_csv_out(logger, config, file):
    """
    Writes a Pandas dataframe object to the local file system
    :param object logger: Logging object
    :param dict config: The config file loaded as a dictionary
    :param Dataframe file: Pandas Dataframe object with the data to export as CSV
    :return: None
    """
    if not os.path.exists("./output"):
        os.mkdir("./output")
    filename = f"{config['metadata_file_name']}-{datetime.now()}.csv"
    logger.info(f"Exporting metadata to local file system as {os.getcwd()}/output/{filename}...")
    file.to_csv(f"./output/{filename}", index=False)


def validate_config_file(config):
    expected_fields = ["buckets_to_read", "bucket_to_write_to", "metadata_file_name",
                       "metadata_destination_directory", "dq_file_name", "dq_destination_directory"]
    if list(config.keys()).sort() != expected_fields.sort():
        print("Invalid Config file format, incorrect keys supplied in JSON. Exiting.")
        sys.exit()
    if not isinstance(config["buckets_to_read"], list):
        print(f"Invalid Config file format, Buckets to read field should be supplied as array, "
              f"not {type(config['buckets_to_read'])}. Exiting.")
        sys.exit()
