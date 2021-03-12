"""
Demo of how to list out dataset files for a new manifest files

Notes:
    - Event trigger is path to the uploaded manifest file, and bucket name
"""

import boto3
import json
from os.path import dirname


def get_corresponding_dataset_files(bucket: str,
                                    manifest_file: str
                                    ) -> list:
    """
    List out corresponding datast files for passed manifest file
    :param bucket: bucket the manifest file is in e.g. "elms-test-1"
    :param manifest_file: location of the manifest file in the bucket e.g. "FBS/manifest/manifest.json.json"
    :return: nothing
    """
    # local credentials for demo
    with open("C:/Users/beellis/aws_creds.json") as cf:
        aws_credentials = json.load(cf)

    # get name of the directory in which the corresponding dataset files are kept
    corresponding_dataset_dir = "{}/{}/".format(dirname(manifest_file).split("/")[0], "data")

    # set up client for s3 buckets, might want to set up this client outside def
    client = boto3.client('s3',
                          aws_access_key_id = aws_credentials["aws_access_key_id"],
                          aws_secret_access_key = aws_credentials["aws_secret_access_key"]
                          )

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket = bucket,
                                       Prefix = corresponding_dataset_dir)

    # lists all the dataset files in corresponding_dataset_dir
    try:
        contents = [page['Contents'] for page in page_iterator]
        # loop through evey page
        for entry in contents:
            # loop through evey item in a page
            for object in entry:
                print(object)
        return contents
    except KeyError:
        print("ERROR: got no dataset files for manifest file {}".format(manifest_file))


def data_ingest_dispatch():
    pass


if __name__ == "__main__":

    # demo of the above def

    bucket = "elms-test-1"
    manifest_files = ["FBS/manifest/manifest.json.json",
                      "National_Parks_England_JSON/manifest/manifest.json"
                      ]

    for manifest_file in manifest_files:
        print("For manifest file {} got dataset file(s):".format(manifest_file))
        get_corresponding_dataset_files(bucket = bucket,
                                        manifest_file = manifest_file)

    bucket = "elms-test-2"
    manifest_files = ["farm_wildlife_package_hotspots/manifest/manifest.json.json",
                      "NE_NationalParksEngland_SHP_Full/manifest/manifest.json",
                      "NE_NationalParkEngland_SHP_Full/manifest/manifest.json"
                      ]

    for manifest_file in manifest_files:
        print("For manifest file {} got dataset file(s):".format(manifest_file))
        get_corresponding_dataset_files(bucket = bucket,
                                        manifest_file = manifest_file)
