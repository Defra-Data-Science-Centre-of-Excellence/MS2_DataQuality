"""
TODO:
    - could this be done in parallel? I think we should use threading
    - add in audit statments so progress can be tracked for logs
    -
"""

from app.Crawler.CloudDataStorageManager import CloudDataStorageManager
from app.dataHandlers import *
from os.path import dirname, splitext
from typing import Union


class Crawler(object):
    """

    """

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
        dataset_files = self._cdsm.get_dataset_files_list(bucket = bucket)

        for dataset_file in dataset_files:
            print(f"Creating metadata file for dataset file {dataset_file['Key']}")
            dataset_dir_name = dirname(dataset_file["Key"]).split("/")[0]
            manifest_directory = f"{dataset_dir_name}/manifest/"
            manifest_file = self._cdsm.get_manifest_file(bucket = bucket, manifest_directory = manifest_directory)
            if manifest_file is None:
                print(f"ERROR: no manifest file returned, creation of metadata file for dataset file {dataset_file['Key']} aborted")
                   # this needs to be tested
            else:
                created_dataset_metadata = self._create_dataset_file_metadata(bucket = bucket, dataset_file = dataset_file)
                if created_dataset_metadata is None:
                    print(f"WARNING: unable to create some metdata for dataset file {dataset_file['Key']}")
                else:
                    # here we need to combine the created metdata and the manifest metdata
                    pass

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

    def _create_dataset_file_metadata(self, bucket: str, dataset_file: dict) -> Union[list, None]:
        """
        Loads file from storage and returns the metadata
        TODO - how do we return with and without layers
        :return:
        """
        shape_file_formats = [".shp", ".shx", ".shb"]
        _, dataset_file_extension = splitext(dataset_file["Key"])
        dataset_file_flo = self._cdsm.read_file_from_storage(bucket = bucket, key = dataset_file["Key"])
        if dataset_file_extension in shape_file_formats:
            # disspacth to shape file data handler
            print(f"ERROR: dataset file is Shape file format, this is currently not supported")
            return None
        elif dataset_file_extension == ".json":
            try:
                header_list, num_rows = create_geojson_metadata(file = dataset_file_flo)
                return [header_list, num_rows]
            except:
                # this except is too broad
                print(f"ERROR: tried to load file {dataset_file} as GEOjson but failed. Only GEOjson formats of json files are currently supported")
                return None
        elif dataset_file_extension == ".csv":
            # at present csv handler can't parse the boto3 FLOs
            try:
                header_list, num_rows = create_csv_metadata(file = dataset_file_flo)
                return [header_list, num_rows]
            except:
                # except is too broad
                return None
        elif dataset_file_extension == ".gpkg":
            try:
                layers, headers_list, num_rows = create_gpkg_metadata(file = dataset_file_flo)
                return [layers, headers_list, num_rows]
            except:
                # except is too broad
                return None
        else:
            print(f"ERROR: did not recognise file extension {dataset_file_extension} for file {dataset_file['Key']}")
            return None

    def __str__(self):
        return "Crawler Object"

    def __repr__(self):
        return self.__str__()


if __name__ == "__main__":
    bucket = "elms-test-2"
    c = Crawler(credentials_fp = "C:/Users/beellis/aws_creds.json")
    c.create_metadata_for_bucket(bucket = bucket)