"""
Notes:
    - using pagination which is clumsy but else we're restricted to max 1000 results
    - what is the default manifest file name? is there going to be a default name? if no, do we assume there is only
    one file? Currently this works on the basis of that only one manifest file is present per dataset, if more than
    one is present in the manifest dir this will pass and audit a warning
    - how do we want to return the result? write to storage?
    - how to log errors, i think print() will do this i.e. more than one manifest file
    - change manifest from csv to json
    - will the manifest file always have the same format, fields etc?
    - used caching to reduce calls to the server for the manifest file
    - how to export resultant metdata?
"""

import boto3
from functools import lru_cache
from botocore.client import BaseClient
import re
from os.path import dirname
import json
import uuid


def crawler(bucket: str) -> None:
    """
    Def to crawl buckets and create metadata files
    :return: nothing
    """
    # using a local credentials file
    with open("C:/Users/beellis/aws_creds.json") as cf:
        aws_credentials = json.load(cf)

    client = boto3.client('s3',
                          aws_access_key_id = aws_credentials["aws_access_key_id"],
                          aws_secret_access_key = aws_credentials["aws_secret_access_key"]
                          )

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket = bucket)
    # , Delimiter='/')
    # , Prefix = '/')

    # lists everything in the bucket
    contents = [page['Contents'] for page in page_iterator]

    # loop through evey page
    for entry in contents:
        # loop through evey item in a page
        for object in entry:
            # directroies are returned just like files are but they have a size of 0
            # so here let's only consider files
            if object["Size"] > 1:
                # here we only want to consider the files that are actual dataset files
                if re.match(".*\/data\/.*", object["Key"]):
                    # i think lambda functions collect print statements in the logs
                    print("Creating metadata file for file: {}".format(object["Key"]))
                    # this is the name of the dataset directory for the dataset file
                    dataset_dir_name = dirname(object["Key"]).split("/")[0]
                    # get the directory for the manifest of the dataset
                    manifest_directory = "{}/{}/".format(dataset_dir_name, "manifest")
                    # call a function to create metadata files for each datafile
                    _create_metadata_for_dataset_file(client = client,
                                                      manifest_directory = manifest_directory,
                                                      object = object,
                                                      bucket = bucket)
                else:
                    pass
            else:
                pass


def _create_metadata_for_dataset_file(client: BaseClient,
                                      bucket: str,
                                      manifest_directory: str,
                                      object: dict):
    """
    Creates metadata files
    :param client: boto3 session
    :param bucket: bucket to find manifest file in
    :param manifest_directory: directory to find manifest file in
    :param object: file object
    :return:
    """
    # create a new paginator that gets everything in the manifest directory
    paginator_manifest = client.get_paginator('list_objects')
    page_iterator = paginator_manifest.paginate(Bucket = bucket,
                                                Prefix = manifest_directory)
    # loop through results to find manifest file
    for page in page_iterator:
        # boto3 will return the name of the dir and the name of the file, so if there is only one file as expected
        # the length of page['Contents'] should be 2
        if len(page['Contents']) == 2:
            count = 0
            for entry in page['Contents']:
                count += 1
                if ".csv" in entry["Key"]:
                    manifest_file = _get_manifest_file(client = client,
                                                       bucket = bucket,
                                                       key = entry["Key"])

                    # construct metadata file
                    _construct_metadata_file(object = object,
                                             manifest_file = manifest_file,
                                             )
                    # generate uuid
                    # get size of file

                else:
                    # only audit this if both paths have been checked and the manifest file doesn't have the expected
                    # extension
                    if count == 2:
                        print("ERROR: could not find manafest file with expected extension .json in the location: {}"
                              .format(manifest_directory))

        # auditing errors for not finding manifest file
        elif len(page['Contents']) > 2:
            print("ERROR: found more than one manifest file in folder {}".format(page['Contents'][0]["Key"]))
        else:
            print("ERROR: no manifest file found in folder {}".format(page['Contents'][0]["Key"]))


@lru_cache(maxsize = 2)
def _get_manifest_file(client: BaseClient,
                       bucket: str,
                       key: str):
    """
    Gets manifest file from bucket, using cache to reduce calls to the server
    :param client:
    :param bucket:
    :param key:
    :return:
    """
    obj = client.get_object(Bucket = bucket, Key = key)
    # used to read dummy file, this needs to be json
    manifest_file = pd.read_csv(obj['Body'])
    return manifest_file


def _construct_metadata_file(object: dict,
                             manifest_file: dict) -> dict:
    metadata_file = {}
    # add filename
    metadata_file["File Name"] = object["Key"]
    # create ID
    metadata_file["ID"] = "ELM-{}".format(uuid.uuid4().hex)
    # add size of file
    metadata_file["File Size"] = object["Size"]

    return metadata_file


if __name__ == "__main__":
    bucket_test = "bene-test-1"
    crawler(bucket = bucket_test)
