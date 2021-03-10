"""
TODO
    - Check assumption: passing a json credentials file
    - Implement upload functionality
    - Test get manifest file functionaltiy
    - implement _load_manifest_file for json
    - what happens if you try to acesss a bucket that doesn't exist, this should be auidted
    - docstrings
    - verify caching is correct
"""

import boto3
import json
import re
from functools import lru_cache
from typing import Union


class CloudDataStorageManager(object):
    """
    CloudDataStorageManager is a class used to handle interactions with AWS S3 buckets

    Usage:

    cdsm = CloudDataStorageManager(credentials_fp = "some/fp/here")

    all_dataset_file = cdsm.get_dataset_files_list(bucket = "elms-test-1")

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

    def get_dataset_files_list(self, bucket: str) -> list:

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

    def get_manifest_file(self,
                          bucket: str,
                          manifest_directory: str) -> Union[None, dict]:
        """
        Method to return the manifest file requested as a FLO
        :param bucket:
        :param manifest_directory:
        :return:
        """
        page_iterator = self._list_object_paginator.paginate(Bucket = bucket,
                                                             Prefix = manifest_directory)
        for page in page_iterator:

            if len(page['Contents']) == 2:
                count = 0

                for entry in page['Contents']:
                    count += 1

                    if "manifest.json" in entry["Key"]:
                        manifest_file = self._load_manifest_file(bucket = bucket,
                                                                 key = entry["Key"])
                        return manifest_file

                    else:
                        # only audit this if both paths have been checked and the manifest file doesn't have the
                        # expected name and extension
                        if count == 2:
                            print(
                                f"WARNING: could not find manifest file manifest.json in directory {manifest_directory}")

            # auditing errors for not finding manifest file
            elif len(page['Contents']) > 2:
                print(f"WARNING: found more than one manifest file in directory {manifest_directory}")

            else:
                print(f"WARNING: no manifest file found in directory {manifest_directory}")

    def read_file_from_storage(self, bucket: str, key: str) -> str:
        """

        :param bucket:
        :param key:
        :return: FLO object
        """
        obj = self._client.get_object(Bucket = bucket, Key = key)
        return obj['Body']

    @lru_cache(maxsize = 2)
    def _load_manifest_file(self, bucket: str, key: str) -> dict:
        """

        :param bucket:
        :param key:
        :return: dict
        """
        manifest_flo = self.read_file_from_storage(bucket = bucket, key = key)
        return json.load(manifest_flo)

    def __str__(self):
        return "CloudDataStorageManager Object"

    def __repr__(self):
        return self.__str__()


if __name__ == "__main__":
    cdsm = CloudDataStorageManager(credentials_fp = "C:/Users/beellis/aws_creds.json")
    dataset_file = cdsm.get_dataset_files_list(bucket = "elms-test-1")
