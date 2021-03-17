from app.dataHandlers.gpkg_handling import create_geospatial_metadata_and_dq
from app.data_quality import create_dq_reports


def create_shape_metadata(file):
    """
    Create metadata for single layer shape files loaded in from zip files
    :param file: filepath
    :return: header_list - list of column names, num_rows - int number of rows
    """
    _, headers_list, num_rows = create_geospatial_metadata_and_dq(file, type = "shp", output = "metadata")
    header_list = headers_list[0]
    num_rows = int(num_rows[0])
    return header_list, num_rows


def create_shape_data_quality_report(file: str, last_modified: str) -> list:
    """
    Function to return data quality metrics for zip shp files
    :param file:
    :return:
    """
    gdf_list = create_geospatial_metadata_and_dq(file, type = "shp", output = "dq")
    dq_df = create_dq_reports(gdf_list, last_modified)
    return dq_df
