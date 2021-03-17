from app.dataHandlers.geospatial_handling import create_geospatial_metadata_and_dq
from app.data_quality import create_dq_reports


def create_gpkg_metadata(file) -> tuple:
    """
    Function to extarct headers and rows for all layers from a geopackage
    :param file: File-like object geopackage file
    :return: layers - a list of the layer names,
             headers_list - a list of lists where each index is a list of the column names for the
                            layer name in layers at the same index
             num_rows - a list where the index of the layer is the number of rows that layer has
    """
    return create_geospatial_metadata_and_dq(file, type = "gpkg", output = "metadata")


def create_gpkg_data_quality_report(file: bytes, last_modified: str) -> list:
    """
    Function to return data quality metrics for gpkg files
    :param file:
    :return:
    """
    gdf_list = create_geospatial_metadata_and_dq(file, type = "gpkg", output = "dq")
    dq_df = create_dq_reports(gdf_list, last_modified)
    return dq_df
