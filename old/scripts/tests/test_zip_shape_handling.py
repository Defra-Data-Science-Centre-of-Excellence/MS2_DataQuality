from scripts.dataHandlers import create_shape_metadata, create_shape_data_quality_report
from scripts.main_aux import load_json_file
from unittest import TestCase
import os
import shutil
import logging
import pandas as pd


class testSHPHandler(TestCase):

    def setUp(self) -> None:
        if not os.path.exists("./temp"):
            os.makedirs("./temp")
        test_zip = f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test_shape.zip"
        shutil.copyfile(test_zip, "./temp/test_shape.zip")
        self.test_zip_loc = "./temp/test_shape.zip"
        self.logger = logging.getLogger()
        json_file = load_json_file(
            f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\script_companion.json")
        self.dq_cols = json_file["dq_columns"]
        self.test_dq_df = pd.read_csv(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test_shp_dq.csv")

    def tearDown(self) -> None:
        if os.path.exists("./temp"):
            shutil.rmtree("./temp", ignore_errors = True)
        if os.path.exists("./logs"):
            shutil.rmtree("./logs")

    def test_create_shape_metadata(self):
        """
        Test that the create_shape_metadata outputs the headers and number of rows
        """
        header_list, num_rows = create_shape_metadata(file = self.test_zip_loc)

        self.assertEqual(header_list, ['FID', 'Name', 'Opened', 'Start', 'End_', 'Length_Km', 'Length_Mil',
                                       'Updated', 'Last_VR', 'GlobalID', 'SHAPE_Leng', 'geometry'])
        self.assertEqual(num_rows, 13)

    def testcreate_shape_data_quality_report(self):
        """
        Test that create_shape_data_quality_report returns the expected output
        """
        data = create_shape_data_quality_report(logger = self.logger, file = self.test_zip_loc,
                                                dataset_file = {"Key": "test_shape/test_shape.shp",
                                                                "LastModified": "24/03/21"})
        data = data[0]
        df = pd.DataFrame.from_records(data, columns = self.dq_cols)
        df.drop(["ReportGenerated"], axis = 1, inplace = True)

        self.assertTrue(df.equals(self.test_dq_df))
