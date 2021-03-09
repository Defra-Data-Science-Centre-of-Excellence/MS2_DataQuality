"""
TODO:
    - could this be done in parallel?
    - I need to move the dataHandlers into the crawler packae? I can't access them as is
"""

from .CloudDataStorageManager import CloudDataStorageManager
from os.path import dirname


class Crawler(object):

    def __init__(self, credentials_fp: str):
        """
        Constructor
        """
        self._cdsm = CloudDataStorageManager(credentials_fp = credentials_fp)

    def create_metadata_for_bucket(self, bucket: str) -> None:
        """
        Create metadata for one bucket
        :param bucket:
        :return:
        """
        dataset_files = self._cdsm.get_dataset_files(bucket = bucket)

        for dataset_file in dataset_files:
            dataset_dir_name = dirname(dataset_file["Key"]).split("/")[0]
            manifest_directory = "{}/{}/".format(dataset_dir_name, "manifest")
            manifest_file = self._cdsm.get_manifest_file(bucket = bucket, manifest_directory = manifest_directory)
            # know we need to create the metadata

    def create_metadata_for_buckets(self, buckets: list) -> None:
        """
        Create metadata for a list of buckets
        :param buckets:
        :return:
        """
        for bucket in buckets:
            self.create_metadata_for_bucket(bucket = bucket)

    def create_data_quality_for_bucket(self, bucket):
        # TODO build sprint 4
        pass

    def create_data_quality_for_buckets(self, bucket: list):
        # TODO build sprint 4
        for bucket in bucket:
            self.create_data_quality_for_bucket(bucket = bucket)