from app.dataHandlers.gpkg_handling import create_geospatial_metadata


def create_shape_metadata(file):
    """
    Create metadata for single layer shape files loaded in from zip files
    :param file: filepath
    :return: header_list - list of column names, num_rows - int number of rows
    """
    _, headers_list, num_rows = create_geospatial_metadata(file, type = "shp")
    header_list = headers_list[0]
    num_rows = int(num_rows[0])
    return header_list, num_rows


def create_shape_data_quality_report(file):
    # TODO build sprint 4
    pass
