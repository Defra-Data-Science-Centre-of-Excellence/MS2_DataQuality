import geopandas
import fiona
from uuid import uuid4
from os.path import exists
from os import mkdir, remove


def create_geospatial_metadata(file, type: str) -> tuple:
    """
    Function to extract headers and rows for all layers from geosaptial data formats

    Supported formats:
        - GeoPackage
        - GEOjson
        - zipped shape files stored in local directories

    :param file: File-like object geopackage file
    :param type: File type / format
    :return: layers       - a list of the layer names, if the file is GEOjson layer is a list of None
             headers_list - a list of lists where the list at index n is the list of the headers for the layer at index
                            n of layers
             num_rows     - a list where the value at index n is the number of rows for the layer at the index n
                            of layers
    """

    if type == "GEOjson":
        # convert bytes to string
        all_layers = [None]
        file = file.decode("utf-8")
    elif type == "gpkg":
        # wrtie out to local temp directory work around
        if not exists("./temp"):
            mkdir("./temp")
        local_name = f"./temp/{uuid4().hex}.gpkg"
        f = open(local_name, 'wb')
        f.write(file)
        f.close()
        all_layers = fiona.listlayers(local_name)
        file = local_name
    elif type == "shp":
        file_to_rm = file
        file = f"zip://{file}"
        all_layers = [None]
    else:
        raise ValueError(f"File format {type} not recognised , at present only GEOjson and gpkg file formats "
                         f"are supported")
    layers = []
    headers_list = []
    num_rows = []

    for layer in all_layers:
        try:
            gdf = geopandas.read_file(file, layer = layer)
            layers.append(layer)
            gdf_col = [col for col in gdf.columns]
            headers_list.append(gdf_col)
            num_rows.append(len(gdf))
        except Exception as e:
            print(e)
            # this except clause is too broad
            print(f"ERROR: couldn't load layer {layer}")

    if type == "gpkg":
        remove(file)
    elif type == "shp":
        remove(file_to_rm)

    return layers, headers_list, num_rows
