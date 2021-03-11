"""
TODO:
    - could this be done in parallel? I think we should use threading
"""

from app.Crawler.CloudDataStorageManager import CloudDataStorageManager
from app.dataHandlers import *
from os.path import dirname, splitext
import os
from typing import Union
import time
import pandas as pd
from app.main_aux import load_json_file


class Crawler(object):
    """
    Class to crawl through buckets and create metadata or data quality reports
    """

    def __init__(self, credentials_fp: str):
        """
        Constructor
        Sets up an instance of CloudDataStorageManager interact with S3 buckets
        """
        self._cdsm = CloudDataStorageManager(credentials_fp = credentials_fp)
        # TODO link this to main script and have companion file passed in as __init__ param
        self._companion_json = load_json_file(f"{os.getcwd()}/app/script_companion.json")

    def create_metadata_for_bucket(self, bucket: str) -> None:
        """
        Create metadata for one bucket
        :param bucket: bucket to create metadata for
        :return: nothing
        """
        dataset_files = self._cdsm.get_dataset_files_list(bucket = bucket)

        if dataset_files is None:
            # no bucket exists so we get no returned files
            print(f"ERROR: aborting metadata creation for bucket {bucket}")
        else:
            for dataset_file in dataset_files:
                print(f"Creating metadata file for dataset file {dataset_file['Key']}")
                dataset_dir_name = dirname(dataset_file["Key"]).split("/")[0]
                manifest_directory = f"{dataset_dir_name}/manifest/"
                manifest_file = self._cdsm.get_manifest_file(bucket = bucket, manifest_directory = manifest_directory)
                csv_data = []

                if manifest_file is None:
                    print(f"ERROR: no manifest file returned, creation of metadata file for dataset file {dataset_file['Key']} aborted")

                else:
                    created_dataset_metadata = self._create_dataset_file_metadata(bucket = bucket, dataset_file = dataset_file)

                    if created_dataset_metadata is None:
                        print(f"WARNING: unable to create some metadata for dataset file {dataset_file['Key']}")
                    else:
                        # here we need to combine the created metdata and the manifest metdata
                        metadata_row = []
                        generated_fields = {"file_url": dataset_dir_name,
                                            "data_last_updated": "",
                                            "column_names": "",
                                            "row_count": "",
                                            "geo_layers": "",
                                            "file_size": "",
                                            "file_extensions": ""}
                        for k, v in self._companion_json["metadata_columns"]:
                            if k in manifest_file.keys():
                                metadata_row.append(manifest_file[k])
                            elif k in generated_fields.keys():
                                metadata_row.append(generated_fields[k])
                        csv_data.append(metadata_row)

                    export_columns = self._companion_json["metadata_columns"].values()
                    export_df = pd.DataFrame(columns=export_columns, data=csv_data)
                    # TODO put in check if output folder exists
                    export_df.to_csv(f"{os.getcwd()}/output/elms-metadata.csv")

                    # So here is where we need to combine:
                    #   - the manifest file, variable: manifest_file - a dictionary format of the manifest file
                    #   - aws metadata (e.g. datetime last updated, file_size), variable: dataset_file - a dictionary
                    #   - the metadata we create by parsing, variable: created_dataset_metadata - a list of lists
                    #     If the dataset file has layers the created_dataset_metadata looks like [layers, headers_list, num_rows]
                    #     where layers is a list of lists with every entry being the name of the layaer, headers_list is
                    #     is a list of lists where each list contains the headers for the layer at the same index and num rows
                    #     is a list of the number of rows.
                    #     If the dataset files doesn't have layers (i.e. geojson or csv) the created_dataset_metadata looks like
                    #     [header_list, num_rows] where header_list is a list of the headers and num_rows is an integer, the
                    #     number of rows

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
        Create metadata for a dataset file, by loading the file into memory and parsing headers, layers, and numbers of
        rows
        Note: this is a private method, it should only be accessed by the class

        :param bucket: bucket the dataset file is in
        :param dataset_file: location of the dataset file in the bucket
        :return: list - of layers (optional depending on format), headers, and number of rows, if the file can't be
        parsed None is returned
        """
        shape_file_formats = [".shp", ".shx", ".shb", ".cpg", ".dbf", ".prj", ".sbn", ".sbx", ".shp.xml"]  # TODO call from script_companion.json
        _, dataset_file_extension = splitext(dataset_file["Key"])
        dataset_file_flo = self._cdsm.read_file_from_storage(bucket = bucket, key = dataset_file["Key"])

        if dataset_file_extension in shape_file_formats:
            print(f"ERROR: dataset file is Shape file format, this is currently not supported")
            return None

        elif dataset_file_extension == ".json":
            try:
                header_list, num_rows = create_geojson_metadata(file = dataset_file_flo)
                return [header_list, num_rows]

            except Exception as e:
                print(e)
                print(f"ERROR: tried to load file {dataset_file} as GEOjson but failed. Only GEOjson "
                      f"formats of json files are currently supported")
                return None

        elif dataset_file_extension == ".csv":
            try:
                header_list, num_rows = create_csv_metadata(file = dataset_file_flo)
                return [header_list, num_rows]

            except Exception as e:
                print(e)
                return None

        elif dataset_file_extension == ".gpkg":
            try:
                layers, headers_list, num_rows = create_gpkg_metadata(file = dataset_file_flo)
                return [layers, headers_list, num_rows]

            except Exception as e:
                print(e)
                return None

        else:
            print(f"ERROR: did not recognise file extension {dataset_file_extension} for file {dataset_file['Key']}")
            return None

    def __str__(self):
        return "Crawler Object"

    def __repr__(self):
        return self.__str__()


if __name__ == "__main__":
    s = time.time()
    bucket = "elms-test-1"
    c = Crawler(credentials_fp = "C:/Users/beellis/aws_creds.json")
    c.create_metadata_for_bucket(bucket = bucket)
    e = time.time() - s
    print(e)