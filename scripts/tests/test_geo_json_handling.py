from scripts.dataHandlers import create_geojson_metadata, create_geojson_data_quality_report
from unittest import TestCase
import os
import logging
from scripts.main_aux import load_json_file
import pandas as pd


class testGEOJsonHandler(TestCase):
    """
    Testing for the functions in the geojson_handing package
    """

    def setUp(self) -> None:
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test.json",
                  "rb") as f:
            test_geojson = f.read()
        self.test_geojson = test_geojson
        self.logger = logging.getLogger()
        json_file = load_json_file(
            f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\script_companion.json")
        self.dq_cols = json_file["dq_columns"]

    def test_create_geojson_metadata(self):
        """
        Test that the create_csv_metadata_function outputs the list of layers, headers and number of rows
        :return:
        """
        header_list, num_rows = create_geojson_metadata(file = self.test_geojson)


        self.assertEqual(header_list, ['OBJECTID', 'name', 'opened', 'start', 'end_', 'length_km', 'length_mil',
                                       'updated', 'last_vr', 'Shape_Length', 'geometry'])
        self.assertEqual(num_rows, 4)

    def test_create_geojson_data_quality_report(self):
        data = create_geojson_data_quality_report(logger = self.logger,
                                                  file = self.test_geojson,
                                                  dataset_file = {"Key": "test_csv/test.csv",
                                                                  "LastModified": "24/03/21"})
        print(data)
        df = pd.DataFrame.from_records(data, columns = self.dq_cols)
        df.to_csv("output.csv", index = False, index_label = False)
