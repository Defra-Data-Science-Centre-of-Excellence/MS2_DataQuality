from scripts.dataHandlers import create_geojson_metadata, create_geojson_data_quality_report
from unittest import TestCase
import os
import logging
from scripts.main_aux import load_json_file
import pandas as pd
import shutil


class testGEOJsonHandler(TestCase):

    def setUp(self) -> None:
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test.json",
                  "rb") as f:
            test_geojson = f.read()
        self.test_geojson = test_geojson
        self.logger = logging.getLogger()
        json_file = load_json_file(
            f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\script_companion.json")
        self.test_dq_df = pd.read_csv(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test_json_dq.csv")
        self.dq_cols = json_file["dq_columns"]

    def tearDown(self) -> None:
        if os.path.exists("./logs"):
            shutil.rmtree("./logs")

    def test_create_geojson_metadata(self):
        """
        Test that the create_geojson_metadata outputs the list of layers, headers and number of rows
        """
        header_list, num_rows = create_geojson_metadata(file = self.test_geojson)

        self.assertEqual(header_list, ['OBJECTID', 'name', 'opened', 'start', 'end_', 'length_km', 'length_mil',
                                       'updated', 'last_vr', 'Shape_Length', 'geometry'])
        self.assertEqual(num_rows, 4)

    def test_create_geojson_data_quality_report(self):
        """
        Test that the create_geojson_data_quality_report returns correct result
        """
        data = create_geojson_data_quality_report(logger = self.logger,
                                                  file = self.test_geojson,
                                                  dataset_file = {"Key": "test_csv/test.csv",
                                                                  "LastModified": "24/03/21"})
        data = data[0]
        df = pd.DataFrame.from_records(data, columns = self.dq_cols)
        df.drop(["ReportGenerated"], axis = 1, inplace = True)
        self.assertTrue(df.equals(self.test_dq_df))
