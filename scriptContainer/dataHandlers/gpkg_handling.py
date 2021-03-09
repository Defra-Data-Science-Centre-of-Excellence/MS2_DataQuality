import geopandas
import fiona


def create_gpkg_metadata(file) -> tuple:
    """
    Function to extarct headers and rows for all layers from a geopackage
    :param file: File-like object geopackage file
    :return: layers - a list of the layer names,
             headers_list - a list of lists where each index is a list of the column names for the
                            layer name in layers at the same index
             num_rows - a list where the index of the layer is the number of rows that layer has
    """

    layers = []
    headers_list = []
    num_rows = []

    for layer in fiona.listlayers(file):
        layers.append(layer)
        gdf = geopandas.read_file(file, layer = layer)

        gdf_col = [col for col in gdf.columns]
        headers_list.append(gdf_col)
        num_rows.append(len(gdf))

    return layers, headers_list, num_rows


def create_gpkg_data_quality_report(file):
    # TODO build sprint 4
    pass
