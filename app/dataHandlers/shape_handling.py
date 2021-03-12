from app.dataHandlers.gpkg_handling import create_geospatial_metadata


def create_shape_metadata(file):
    return create_geospatial_metadata(file, type = "shape")


def create_shape_data_quality_report(file):
    # TODO build sprint 4
    pass
