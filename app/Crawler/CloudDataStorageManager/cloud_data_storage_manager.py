"""
TODO
    - Implement upload functionality
"""

import boto3
import json
import re
from functools import lru_cache
from typing import Union
from Crawler.CloudDataStorageManager.abc import CloudDataStorageManagerABC


class CloudDataStorageManagerAWS(CloudDataStorageManagerABC):
    """
    CloudDataStorageManagerAWS is a class used to handle interactions with AWS S3 buckets

    Note: This class is implemented in a cloud-agnostic manner, all returns are of python native types so to move to a
    different cloud storage solution for data, only this class would need to be reimplemented

    Usage:

    cdsm = CloudDataStorageManager(logger=logger, credentials_fp="some/fp/here")

    all_dataset_file = cdsm.get_dataset_files_list(bucket = "elms-test-1")

    """

    def __init__(self, logger):
        """
        Constructor
        Sets up
            - BaseClient object to interact with s3 buckets
            - Reusable paginator to list objects in s3 storage

        :param logger: logging object
        :param credentials_fp: file path for aws credentials file
        """
        self.logger = logger

        self._client = boto3.client('s3')
        self._s3_resource = boto3.resource('s3')
        self._list_object_paginator = self._client.get_paginator('list_objects')

    def export_file_to_s3(self, bucket, metadata_destination, file_data) -> None:
        """
        Method to upload the metadata file to an S3 bucket
        :param string bucket: Name of the bucket to upload metadata file to
        :param string metadata_destination: directory + file name within an S3 bucket to upload the metadata file to
        :param StringIO file_data: StringIO buffer of a pandas dataframe containing the metadata file data
        :return: None
        """
        self._s3_resource.Object(bucket, metadata_destination).put(Body=file_data)

    def get_dataset_files_list(self, bucket: str) -> Union[None, list]:
        """
        Get all dataset files in the specified bucket
        :param bucket: Bucket name
        :return: list of dictionaries, with each dictionary containing the file path and additional metadata for a
        datafile
        """
        self.logger.debug(f"Paginating objects in bucket {bucket}")
        page_iterator = self._list_object_paginator.paginate(Bucket = bucket)
        try:
            contents = [page['Contents'] for page in page_iterator]
            dataset_files_to_return = []

            for entry in contents:
                # loop through evey item in a page
                for object in entry:
                    # directories are returned just like files are but they have a size of 0
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

        except Exception as e:
            self.logger.warning(f"WARNING: got AWS paginator error for bucket {bucket}")
            self.logger.exception(e)
            return None

    def get_manifest_file(self,
                          bucket: str,
                          manifest_directory: str) -> Union[None, dict]:
        """
        Method to return the manifest file requested as a FLO
        :param bucket: bucket the manifest file is in
        :param manifest_directory: directory the manifest file is in
        :return: None if the manifest file cannot be found or a dictionary of the manifest file in memory
        """
        page_iterator = self._list_object_paginator.paginate(Bucket = bucket,
                                                             Prefix = manifest_directory)
        for page in page_iterator:
            if len(page['Contents']) in [1, 2]:
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
                            self.logger.warning(
                                f"WARNING: could not find manifest file manifest.json in directory "
                                f"{manifest_directory}")

            # auditing errors for not finding manifest file
            elif len(page['Contents']) > 2:
                self.logger.debug(f"WARNING: found more than one manifest file in directory {manifest_directory}")

            else:
                self.logger.debug(f"WARNING: no manifest file found in directory {manifest_directory}")

    def read_file_from_storage(self, bucket: str, key: str) -> bytes:
        """
        Read a file from S3 storage
        :param bucket: bucket the file is in
        :param key: the location of the file in the bucket
        :return: bytes - file in memory
        """
        self.logger.debug(f"Retrieving file {key}")
        obj = self._client.get_object(Bucket = bucket, Key = key)
        return obj['Body'].read()

    @lru_cache(maxsize = 2)
    def _load_manifest_file(self, bucket: str, key: str) -> dict:
        """
        Load manifest file into memory as dictionary
        Note: using caching to reduce repeated calls to server for the same manifest file
        :param bucket: bucket the manifest is in
        :param key: the location of the manifest file in the bucket
        :return: dict - the manifest as a python native dictionary
        """
        manifest_flo = self.read_file_from_storage(bucket = bucket, key = key)
        return json.loads(manifest_flo)

    def __str__(self):
        return "CloudDataStorageManager Object"

    def __repr__(self):
        return self.__str__()
