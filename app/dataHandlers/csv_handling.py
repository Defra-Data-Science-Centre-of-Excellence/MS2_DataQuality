import csv


def create_csv_metadata(file):
    """
    Function to extract headers and rows from a CSV file
    :param file: File-like object of a CSV file following a '.readlines' conversion
    :return list; int header_list; num_rows:
    """
    lines = file.decode("utf-8").split()
    reader = csv.reader(lines)
    data = [row for row in reader]
    header_list = data[0]
    num_rows = len(data) - 1  # minus one to correct for headers
    return header_list, num_rows


def create_csv_data_quality_report(file):
    # TODO build sprint 4
    pass
