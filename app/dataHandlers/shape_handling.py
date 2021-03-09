from app.dataHandlers.gpkg_handling import create_geospatial_metadata


def create_shape_metadata(file):
    return create_geospatial_metadata(file, type = "shape")


if __name__ == "__main__":
    create_shape_metadata("C:/Users/beellis/Desktop/Special_Protection_Areas_England.shp")
