import csv


def create_csv_metadata(file):
    """
    Function to extract headers and rows from a CSV file
    :param file: File-like object of a CSV file
    :return list; int header_list; num_rows:
    """
    reader = csv.reader(file)
    data = reader(list(reader))
    header_list = next(reader)
    num_rows = len(data)

    return header_list, num_rows


def create_csv_data_quality_report(file):
    # TODO build sprint 4
    pass
