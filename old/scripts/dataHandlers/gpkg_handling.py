from scripts.dataHandlers.geospatial_handling import create_geospatial_metadata_and_dq
from scripts.dataQuality import create_dq_reports


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


def create_gpkg_data_quality_report(logger, file: bytes, dataset_file: dict) -> list:
    """
    Function to return data quality metrics for gpkg files
    :param logger: a logger object
    :param file: gpkg files in bytes
    :param dataset_file: dictionary containing "Key" and "LastModified" where the key is the filepath and last modifed
    the date the file was last modified
    :return: a list of lists of lists, containing rows of the dq report
    """
    gdf_list = create_geospatial_metadata_and_dq(file, type = "gpkg", output = "dq")
    dq_df = create_dq_reports(logger, gdf_list, dataset_file)
    return dq_df