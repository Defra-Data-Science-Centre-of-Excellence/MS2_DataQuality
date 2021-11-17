from scripts.dataHandlers import create_gpkg_metadata, create_gpkg_data_quality_report
from unittest import TestCase
import os
import shutil
import logging
from scripts.main_aux import load_json_file
import pandas as pd


class testGPKGHandler(TestCase):

    def setUp(self) -> None:
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test.gpkg",
                  "rb") as f:
            test_gpkg = f.read()
        self.test_gpkg = test_gpkg
        self.logger = logging.getLogger()
        json_file = load_json_file(
            f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\script_companion.json")
        self.dq_cols = json_file["dq_columns"]
        self.test_dq_df = pd.read_csv(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test_gpkg_dq.csv")

    def tearDown(self) -> None:
        if os.path.exists("./logs"):
            shutil.rmtree("./logs")

    def test_create_gpkg_metadata(self):
        """
        Test that the create_gpkg_metadata outputs the list of layers, headers and number of rows
        """
        layers, headers_list, num_rows = create_gpkg_metadata(file = self.test_gpkg)
        self.assertEqual(layers, ['test'])
        self.assertEqual(headers_list, [['City', 'Country', 'Latitude', 'Longitude', 'geometry']])
        self.assertEqual(num_rows, [5])

    def test_create_gpkg_data_quality_report(self):
        """
        Test that the create_gpkg_data_quality_report returns correct result
        """
        data = create_gpkg_data_quality_report(logger = self.logger, file = self.test_gpkg,
                                               dataset_file = {"Key": "test_gpkg/test.gpkg",
                                                               "LastModified": "24/03/21"})
        data = data[0]
        df = pd.DataFrame.from_records(data, columns = self.dq_cols)
        df.drop(["ReportGenerated"], axis = 1, inplace = True)
        self.assertTrue(df.equals(self.test_dq_df))
