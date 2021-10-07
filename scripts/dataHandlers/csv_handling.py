import csv
import pandas as pd
from scripts.dataQuality import create_dq_reports
from io import BytesIO


def create_csv_metadata(file: bytes):
    """
    Function to extract headers and rows from a CSV file
    :param file: CSV files in bytes
    :return list; int header_list; num_rows:
    """
    lines = file.decode("utf-8").split()
    reader = csv.reader(lines)
    data = [row for row in reader]
    header_list = data[0]
    num_rows = len(data) - 1  # minus one to correct for headers
    return header_list, num_rows


def create_csv_data_quality_report(logger, file: bytes, dataset_file: dict) -> list:
    """
    Function to load in csv as df to create a dq report
    :param logger: a logger object
    :param file: CSV files in bytes
    :param dataset_file: dictionary containing "Key" and "LastModified" where the key is the filepath and last modifed
    the date the file was last modified
    :return: a list of lists of lists, containing rows of the dq report
    """
    df = pd.read_csv(BytesIO(file), engine='python')
    dq_df = create_dq_reports(logger, gdf_list = [df], file_dict = dataset_file)
    return dq_df
