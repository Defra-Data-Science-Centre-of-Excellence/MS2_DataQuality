"""
ABC for a CloudDataStorageManager with guidance on implementation
"""

from abc import ABC, abstractmethod
from typing import Union


class CloudDataStorageManagerABC(ABC):

    @abstractmethod
    def get_dataset_files_list(self, *args) -> Union[None, list]:
        """
        This method should return a python native list of all dataset files to create metadata for in a specified bucket
        / cloud storage location or None if no files where found in the requested bucket / cloud storage location or
        if the requested bucket / cloud storage locatio doesn't exist

        e.g. in the AWS implementation this method takes one argument:
            - bucket - the name of the bucket the manifest file is in

        :param args: any desired arguments
        :return:     python native list - of all the files in the specified bucket. Each entry in the list should be for a
                     single file and should be a dictionary like:

                             {"Key": "this is the file name",
                            "LastModified": "date of when the file was last modified",
                             "Size": "the size of the file in bytes"}

                     Each dictionary can have additional keys but changes will be need to made in the Crawler class to
                      output these additional keys
        """
        raise NotImplementedError("The abstract method get_dataset_files_list must be implemented")

    @abstractmethod
    def get_manifest_file(self, *args) -> Union[None, dict]:
        """
        This method should return a requested manifest file from a specified cloud storage location

        e.g. in the AWS implementation this method takes two arguments:
            - bucket - the name of the bucket the manifest file is in
            - manifest_directory - the directory in the bucket that the manifest files sits in

        :param args: any desired arguments
        :return:     None - if the requested manifest file does not exist
                     dict - if the manifest file does exist, the required keys can be found in the file
                     script_companion.json
        """
        raise NotImplementedError("The abstract method get_manifest_file must be implemented")

    @abstractmethod
    def read_file_from_storage(self, *args) -> bytes:
        """
        This method  should read a file from a specified cloud storage location

        e.g. in the AWS implementation this method takes two arguments:
            - bucket - the name of the bucket the file to read is in
            - key - the location of the file in the above bucket

        :param args: any desired arguments
        :return:     the file in bytes
        """
        raise NotImplementedError("The abstract method read_file_from_storage must be implemented")
