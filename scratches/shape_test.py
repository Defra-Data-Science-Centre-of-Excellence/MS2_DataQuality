import boto3
import json
import re
import geopandas
from os.path import dirname, splitext
from app.Crawler.CloudDataStorageManager.shape_file_collator import ShapeFileCollator
import shapefile
import os

c_fp = "C:/Users/beellis/aws_creds.json"

with open(c_fp) as cf:
    aws_credentials = json.load(cf)

client = boto3.client('s3',
                      aws_access_key_id = aws_credentials["aws_access_key_id"],
                      aws_secret_access_key = aws_credentials["aws_secret_access_key"]
                      )

list_object_paginator = client.get_paginator('list_objects')

page_iterator = list_object_paginator.paginate(Bucket = "elms-test-2")

contents = [page['Contents'] for page in page_iterator]
dataset_files_to_return = []

for entry in contents:
    # loop through evey item in a page
    for object in entry:
        # directroies are returned just like files are but they have a size of 0
        # so here let's only consider files
        if object["Size"] > 1:
            # here we only want to consider the files that are actual dataset files
            if re.match(".*\/data\/.*", object["Key"]):
                dataset_files_to_return.append(object)

            else:
                pass

        else:
            pass

current_dir = dirname(dataset_files_to_return[0]["Key"]).split("/")[0]
shape_file_dir = None
sfc = None


for dataset_file in dataset_files_to_return:

    dataset_dir_name = dirname(dataset_file["Key"]).split("/")[0]

    if dataset_dir_name != current_dir:
        current_dir = dataset_dir_name
        shape_file_dir = None

    # some of this is reproduced in _create_dataset_file_metadata(), let's clean this at refactor
    shape_file_formats = [".shp", ".shx", ".shb", ".cpg", ".dbf", ".prj", ".sbn", ".sbx",
                          ".shp.xml"]  # TODO call from script_companion.json
    _, dataset_file_extension = splitext(dataset_file["Key"])
    # check if
    if dataset_file_extension in shape_file_formats:
        shape_file_dir = current_dir
        obj = client.get_object(Bucket = "elms-test-2", Key = dataset_file["Key"])
        obj = obj['Body'].read()
        if sfc is None:
            sfc = ShapeFileCollator(dataset_dir = shape_file_dir, cdsm = client)
        else:
            sfc.add_file(file = obj, file_extension = dataset_file_extension)
            if sfc.is_complete():
                print("complete")
                # then try to create the metadata
                zipfile = sfc.zip_complete_file()
                zipfile = f"{os.getcwd()}/{zipfile}"
                gdf = geopandas.read_file(f"zip://{zipfile}")
                print(gdf)
            else:
                pass
    else:
        # do as we used to
        pass







