from scripts.dataHandlers.geospatial_handling import create_geospatial_metadata_and_dq
from scripts.dataQuality import create_dq_reports


def create_geojson_metadata(file: bytes) -> tuple:
    """
    Function to extract headers and rows for all layers from GEOjson data format
    :param file: geojson file in bytes
    :return: header_list - a list of the headers of the file
             num_rows    - number of rows, integer
    """
    _, headers_list, num_rows = create_geospatial_metadata_and_dq(file, type = "GEOjson", output = "metadata")
    header_list = headers_list[0]
    num_rows = int(num_rows[0])
    return header_list, num_rows


def create_geojson_data_quality_report(logger, file: bytes, dataset_file: dict) -> list:
    """
    Function to return data quality metrics for geojson files
    :param logger: a logger object
    :param file: geojson file files in bytes
    :param dataset_file: dictionary containing "Key" and "LastModified" where the key is the filepath and last modifed
    the date the file was last modified
    :return: a list of lists of lists, containing rows of the dq report
    """
    gdf_list = create_geospatial_metadata_and_dq(file, type = "GEOjson", output = "dq")
    dq_df = create_dq_reports(logger, gdf_list, dataset_file)
    return dq_df
