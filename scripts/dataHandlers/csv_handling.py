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
    Function to load in csv as df
    :param file: CSV files in bytes
    :return:
    """
    df = pd.read_csv(BytesIO(file))
    dq_df = create_dq_reports(logger, gdf_list = [df], file_dict = dataset_file)
    return dq_df
