"""
TODO:
    - could this be done in parallel? I think we should use threading
"""

from Crawler.CloudDataStorageManager import CloudDataStorageManagerAWS
from Crawler.CloudDataStorageManager import ShapeFileCollator
from dataHandlers import *
from os.path import dirname, splitext
from typing import Union
import gc


class Crawler(object):
    """
    Class to crawl through buckets and create metadata or data quality reports
    """

    def __init__(self, logger, companion: dict):
        """
        Constructor
        Sets up an instance of CloudDataStorageManager to interact with S3 buckets
        """
        self.logger = logger
        self._cdsm = CloudDataStorageManagerAWS(logger=logger)
        # TODO link this to main script and have companion file passed in as __init__ param
        self._companion_json = companion

    def export_file(self, bucket, export_directory, export_file_name, file_data) -> None:
        """
        Method to upload the metadata file to a cloud storage bucket
        :param string bucket: Name of the bucket to upload metadata file to
        :param string export_directory: directory within an S3 bucket to upload the metadata file to
        :param string export_file_name: file name within an S3 bucket to upload the metadata file to
        :param file_data: StringIO buffer of a pandas dataframe containing the metadata file data
        :return: None
        """
        metadata_destination = f"{export_directory}/{export_file_name}"
        self._cdsm.export_file_to_s3(bucket, metadata_destination, file_data)

    def create_metadata_for_bucket(self, bucket: str) -> list:
        """
        # TODO - this could be a lot cleaner if we spin stuff out to defs or as private methods, would also help with unit testing
        Create metadata for one bucket
        :param bucket: bucket to create metadata for
        :return list csv_data: list of lists containing metadata rows
        """
        dataset_files = self._cdsm.get_dataset_files_list(bucket = bucket)

        if dataset_files is None:
            # no bucket exists so we get no returned files
            self.logger.error(f"ERROR: aborting metadata creation for bucket {bucket}. "
                              f"Bucket is empty or does not exist.")
        else:
            csv_data = []
            sfc = None
            id_num = 0
            for dataset_file in dataset_files:
                id_num += 1
                self.logger.debug(f"Creating metadata for dataset file {dataset_file['Key']}")
                dataset_dir_name = dirname(dataset_file["Key"]).split("/")[0]

                manifest_directory = f"{dataset_dir_name}/manifest/"
                manifest_file = self._cdsm.get_manifest_file(bucket=bucket, manifest_directory=manifest_directory)

                if manifest_file is None:
                    self.logger.debug(f"WARNING: no manifest file returned, creation of metadata file for dataset "
                                      f"file {dataset_file['Key']} aborted.")

                else:
                    dataset_file_extension = self._get_file_extension(dataset_file)

                    if dataset_file_extension in self._companion_json["shape_file_extensions"]:
                        shape_file_dir = dataset_dir_name
                        file = self._cdsm.read_file_from_storage(bucket=bucket, key=dataset_file['Key'])

                        if sfc is None:
                            sfc = ShapeFileCollator(dataset_dir=shape_file_dir)

                        sfc.add_file(file = file,
                                     file_extension=dataset_file_extension,
                                     current_dir = dataset_dir_name,
                                     file_size = dataset_file['Size'])

                        if sfc.is_complete():
                            zipfile, shp_file_size = sfc.zip_complete_file()
                            created_dataset_metadata = self._create_dataset_file_metadata_for_zip(
                                fp=zipfile, format="shape")
                            sfc = None
                            dataset_file["Size"] = shp_file_size
                            # could force gc to release memory here, but it's probably not a huge concern
                            gc.collect()

                        else:
                            continue

                    else:
                        created_dataset_metadata = self._create_dataset_file_metadata(bucket=bucket,
                                                                                      dataset_file=dataset_file)

                    # TODO should this be a if, else??? This way we don't create metadata output files if we don't get
                    # created metadata back. I think if we remove the else here it should work better
                    if created_dataset_metadata is None:
                        self.logger.debug(f"WARNING: unable to create metadata for dataset file "
                                          f"{dataset_file['Key']}")

                    else:
                        # here we need to combine the created metadata and the manifest metadata
                        metadata_row = []
                        if len(created_dataset_metadata) == 2:
                            gen_metadata = {"headers": created_dataset_metadata[0],
                                            "num_rows": created_dataset_metadata[1],
                                            "geo_layers": "N/A"}
                        elif len(created_dataset_metadata) > 2:
                            gen_metadata = {"headers": created_dataset_metadata[1],
                                            "num_rows": created_dataset_metadata[2],
                                            "geo_layers": str(created_dataset_metadata[0])}
                        else:
                            gen_metadata = {"headers": "", "num_rows": "", "geo_layers": ""}

                        generated_fields = {"file_url": dataset_file['Key'],
                                            "data_last_updated": dataset_file['LastModified'],
                                            "column_names": gen_metadata["headers"],
                                            "row_count": gen_metadata["num_rows"],
                                            "geo_layers": gen_metadata["geo_layers"],
                                            "file_size": round(dataset_file['Size']/1048576, 2),  # convert to MB
                                            "file_extensions": dataset_file['Key'].split('.')[-1]}

                        for k, v in self._companion_json["metadata_columns"].items():
                            if k in manifest_file.keys():
                                metadata_row.append(manifest_file[k])
                            elif k in generated_fields.keys():
                                metadata_row.append(generated_fields[k])
                            elif k == 'id':
                                metadata_row.append(id_num)
                            else:
                                metadata_row.append("")
                        csv_data.append(metadata_row)
                        self.logger.info(f"Successfully processed dataset {dataset_dir_name[0]}........"
                                         f"{dataset_dir_name[-1]}.{dataset_file['Key'].split('.')[-1]}")
            return csv_data

    def create_metadata_for_buckets(self, buckets: list) -> list:
        """
        Create metadata for a list of buckets
        :param buckets:
        :return list csv_data: list of lists containing metadata rows from across all buckets specified in the config
        """
        csv_data = []
        for bucket in buckets:
            csv_data.extend(self.create_metadata_for_bucket(bucket = bucket))
        return csv_data

    def create_data_quality_for_bucket(self, bucket):
        # TODO build sprint 4
        pass

    def create_data_quality_for_buckets(self, bucket: list):
        # TODO build sprint 4
        for bucket in bucket:
            self.create_data_quality_for_bucket(bucket = bucket)

    @staticmethod
    def _get_file_extension(dataset_file: dict):
        _, dataset_file_extension = splitext(dataset_file["Key"])
        return dataset_file_extension

    @staticmethod
    def _create_dataset_file_metadata_for_zip(fp: str, format: str) -> Union[list, None]:
        """
        Creates metadata for a zipped dataset saved to a local dir by loading files into memory and parsin headers,
        layers and numbers of rows

        Note: only shape file datasets that are zipped are supported by this method

        Note: this is a private method, it should only be accessed by the lcass
        :return:
        """
        if format == "shape":
            header_list, num_rows = create_shape_metadata(file=fp)
            return [header_list, num_rows]
        else:
            return None

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
        dataset_file_extension = self._get_file_extension(dataset_file)
        dataset_file_flo = self._cdsm.read_file_from_storage(bucket = bucket, key = dataset_file["Key"])

        if dataset_file_extension in self._companion_json["shape_file_extensions"]:
            self.logger.debug(f"ERROR: dataset file is Shape file format, this is currently not supported")
            return None

        elif dataset_file_extension == ".json":
            try:
                header_list, num_rows = create_geojson_metadata(file = dataset_file_flo)
                return [header_list, num_rows]

            except Exception as e:
                self.logger.exception(e)
                self.logger.debug(f"ERROR: tried to load file {dataset_file} as GEOjson but failed. Only GEOjson "
                                  f"formats of json files are currently supported")
                return None

        elif dataset_file_extension == ".csv":
            try:
                header_list, num_rows = create_csv_metadata(file = dataset_file_flo)
                return [header_list, num_rows]

            except Exception as e:
                self.logger.exception(e)
                return None

        elif dataset_file_extension == ".gpkg":
            try:
                layers, headers_list, num_rows = create_gpkg_metadata(file = dataset_file_flo)
                return [layers, headers_list, num_rows]

            except Exception as e:
                self.logger.exception(e)
                return None

        else:
            self.logger.debug(f"ERROR: did not recognise file extension {dataset_file_extension} for "
                              f"file {dataset_file['Key']}")
            return None

    def __str__(self):
        return "Crawler Object"

    def __repr__(self):
        return self.__str__()
