"""
TODO
    - Check assumption: passing a json credentials file
    - Implement upload functionality
    - Test get manifest file functionaltiy
    - implement _load_manifest_file for json
"""

import boto3
from botocore.client import BaseClient
import json
import re
from functools import lru_cache
from typing import Union


class CloudDataStorageManager(object):
    """
    Class to manage cloud data storage.

    Usage:

    cdsm = CloudDataStorageManager(credentials_fp = "some/fp/here",

    all_dataset_file = cdsm.get_dataset_files(bucket = "elms-test-1")

    """

    def __init__(self, credentials_fp: str):

        """
        Constructor
        Sets up
            - BaseClient object to interact with s3 buckets
            - Reusable paginator to list objects in s3 storage

        :param credentials_fp: file path for aws credentials file
        """

        with open(credentials_fp) as cf:
            aws_credentials = json.load(cf)

        self._client = boto3.client('s3',
                                    aws_access_key_id = aws_credentials["aws_access_key_id"],
                                    aws_secret_access_key = aws_credentials["aws_secret_access_key"]
                                    )
        self._list_object_paginator = self._client.get_paginator('list_objects')

    def get_dataset_files(self, bucket: str) -> list:

        """
        Get all dataset files in the specified bucket
        :param bucket: Bucket name
        :return: list of dictionaries, with each dictionary containing the file path and additional metadata for a datafile
        """

        page_iterator = self._list_object_paginator.paginate(Bucket = bucket)
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

        return dataset_files_to_return

    def get_manifest_file(self, bucket: str,
                          manifest_directory: str) -> Union[None, json]:
        """

        :param bucket:
        :param manifest_directory:
        :return:
        """
        page_iterator = self._list_object_paginator.paginate(Bucket = bucket,
                                                             prefix = manifest_directory)
        for page in page_iterator:

            if len(page['Contents']) == 2:

                count = 0

                for entry in page['Contents']:

                    count += 1

                    if entry["Key"] == "manifest.json":

                        manifest_file = _load_manifest_file(client = self._client,
                                                            bucket = bucket,
                                                            key = entry["Key"])
                        return manifest_file
                    else:
                        # only audit this if both paths have been checked and the manifest file doesn't have the
                        # expected name and extension
                        if count == 2:
                            print(
                                "ERROR: could not find manafest file with expected extension .json in the location: {}"
                                .format(manifest_directory))

            # auditing errors for not finding manifest file
            elif len(page['Contents']) > 2:
                print("ERROR: found more than one manifest file in folder {}".format(page['Contents'][0]["Key"]))
            else:
                print("ERROR: no manifest file found in folder {}".format(page['Contents'][0]["Key"]))


@lru_cache(maxsize = 2)
def _load_manifest_file(client: BaseClient,
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


if __name__ == "__main__":
    cdsm = CloudDataStorageManager(credentials_fp = "C:/Users/beellis/aws_creds.json")
    dataset_file = cdsm.get_dataset_files(bucket = "elms-test-1")
    print(dataset_file)
