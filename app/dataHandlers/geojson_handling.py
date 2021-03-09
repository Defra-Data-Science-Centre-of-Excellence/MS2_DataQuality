from app.dataHandlers.geospatial_handling import create_geospatial_metadata


def create_geojson_metadata(file) -> tuple:
    """
    Function to extarct headers and rows for all layers from GEOjson data format
    :param file: File-like object geosaptial file
    :return: header_list - a list of the headers of the file
             num_rows    - number of rows, integer
    """
    _, headers_list, num_rows = create_geospatial_metadata(file, type = "GEOjson")
    header_list = headers_list[0]
    num_rows = int(num_rows[0])
    return header_list, num_rows
