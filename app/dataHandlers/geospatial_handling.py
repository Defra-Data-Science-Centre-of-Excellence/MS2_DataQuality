import geopandas
import fiona


def create_geospatial_metadata(file, type: str) -> tuple:
    """
    Function to extract headers and rows for all layers from geosaptial data formats

    Supported formats:
        - GeoPackage
        - GEOjson

    :param file: File-like object geopackage file
    :param type: File type / format
    :return: layers       - a list of the layer names, if the file is GEOjson layer is a list of None
             headers_list - a list of lists where the list at index n is the list of the headers for the layer at index
                            n of layers
             num_rows     - a list where the value at index n is the number of rows for the layer at the index n
                            of layers
    """

    if type == "GEOjson":
        all_layers = [None]
    elif type == "gpkg":
        all_layers = fiona.listlayers(file)
    else:
        raise ValueError("File format {} not recognised, at present only GEOjson and gpkg files formats are supported".format(type))

    layers = []
    headers_list = []
    num_rows = []

    for layer in all_layers:
        layers.append(layer)
        gdf = geopandas.read_file(file, layer = layer)
        gdf_col = [col for col in gdf.columns]
        headers_list.append(gdf_col)
        num_rows.append(len(gdf))

    return layers, headers_list, num_rows