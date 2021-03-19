from app.dataHandlers.geospatial_handling import create_geospatial_metadata_and_dq
from app.data_quality import create_dq_reports


def create_geojson_metadata(file: bytes) -> tuple:
    """
    Function to extract headers and rows for all layers from GEOjson data format
    :param file: File-like object geosaptial file
    :return: header_list - a list of the headers of the file
             num_rows    - number of rows, integer
    """
    _, headers_list, num_rows = create_geospatial_metadata_and_dq(file, type = "GEOjson", output = "metadata")
    header_list = headers_list[0]
    num_rows = int(num_rows[0])
    return header_list, num_rows


def create_geojson_data_quality_report(file: bytes, dataset_file: dict) -> list:
    """
    Function to return data quality metrics for geojson files
    :param last_modified:
    :param file: flo bytes
    :return:
    """
    gdf_list = create_geospatial_metadata_and_dq(file, type = "GEOjson", output = "dq")
    dq_df = create_dq_reports(gdf_list, dataset_file)
    return dq_df
