import geopandas
import fiona
from uuid import uuid4
from os.path import exists
from os import mkdir, remove
from typing import Union


def create_geospatial_metadata_and_dq(file: Union[bytes, str], type: str, output: str) -> Union[tuple, list]:
    """
    Function to extract headers and rows for all layers from geosaptial data formats and to extract geodataframes

    Supported formats:
        - GeoPackage
        - GEOjson
        - zipped shape files stored in local directories

    :param file: File-like object geopackage file
    :param type: File type / format
    :param output: either "metadata" or "dq"
    :return: If output = metadata:
             layers       - a list of the layer names, if the file is GEOjson layer is a list of None
             headers_list - a list of lists where the list at index n is the list of the headers for the layer at index
                            n of layers
             num_rows     - a list where the value at index n is the number of rows for the layer at the index n
                            of layers
             OR if output = "dq":
             gdf_list - a list where each entry is a geodataframe
    """
    allowed_output = ["metadata", "dq"]
    assert output in allowed_output, f"Expected output to be either 'metadata' or 'dq' but got {output}"

    if type == "GEOjson":
        # convert bytes to string
        all_layers = [None]
        file = file.decode("utf-8")
    elif type == "gpkg":
        # write out to local temp directory work around
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
                         f"are supported.")
    layers = []
    headers_list = []
    num_rows = []

    gdf_list = []

    for layer in all_layers:
        try:
            gdf = geopandas.read_file(file, layer = layer)
            gdf_list.append(gdf)
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

    if output == allowed_output[0]:
        return layers, headers_list, num_rows
    elif output == allowed_output[1]:
        return gdf_list
