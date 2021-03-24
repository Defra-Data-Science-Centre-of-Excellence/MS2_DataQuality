from scripts.dataHandlers.gpkg_handling import create_geospatial_metadata_and_dq
from scripts.dataQuality import create_dq_reports


def create_shape_metadata(file: str) -> tuple:
    """
    Create metadata for single layer shape files loaded in from zip files
    :param file: filepath
    :return: header_list - list of column names, num_rows - int number of rows
    """
    _, headers_list, num_rows = create_geospatial_metadata_and_dq(file, type = "shp", output = "metadata")
    header_list = headers_list[0]
    num_rows = int(num_rows[0])
    return header_list, num_rows


def create_shape_data_quality_report(logger, file: str, dataset_file: dict) -> list:
    """
    Function to return data quality metrics for zip shp files
    :param logger: a logger object
    :param file: CSV files in bytes
    :param dataset_file: dictionary containing "Key" and "LastModified" where the key is the filepath and last modifed
    the date the file was last modified
    :return: a list of lists of lists, containing rows of the dq report
    """
    gdf_list = create_geospatial_metadata_and_dq(file, type = "shp", output = "dq")
    dq_df = create_dq_reports(logger, gdf_list, dataset_file)
    return dq_df
