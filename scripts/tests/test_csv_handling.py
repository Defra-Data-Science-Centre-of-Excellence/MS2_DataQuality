from unittest import TestCase
import os
from scripts.dataHandlers import create_csv_metadata, create_csv_data_quality_report
import logging
import pandas as pd
from scripts.main_aux import load_json_file


class testCsvHandling(TestCase):
    """
    Testing for the functions in the csv_handling package
    """

    def setUp(self) -> None:
        """
        Set up test - load the test csv
        :return:
        """
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test.csv", "rb") as f:
            test_csv = f.read()
        self.test_csv = test_csv
        self.test_dq_df = pd.read_csv(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\tests\\testData\\test_csv_dq.csv")
        self.logger = logging.getLogger()
        json_file = load_json_file(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\scripts\\script_companion.json")
        self.dq_cols = json_file["dq_columns"]

        #self.dq_dtypes = {"Column": "object", "NullPct": "float64", "OneCharacter": "object", "DataTypes": "object",
        #                  "Percent": "object", "Uniqueness": "float64", "ContainsGeometry": "bool", "LastModified": "object",
        #                  "Dataset": "object", "FileExt": "object", "GeomTypesObserved": "object", "InvalidGeometriesAtRows": "object"}
        #self.test_dq_df = self.test_dq_df.astype(self.dq_dtypes)
        self.test_dq_df = self.test_dq_df.fillna('N/A')


    def test_create_csv_metadata(self):
        """
        Test that the create_csv_metadata_function outputs the list of headers and the CSV length
        """
        header_list, num_rows = create_csv_metadata(self.test_csv)
        self.assertEqual(header_list, ['one', 'two', 'three'])
        self.assertEqual(num_rows, 3)

    def test_create_csv_data_quality_report(self):
        """
        Test that the create_csv_data_quality_report returns correct result
        """
        data = create_csv_data_quality_report(logger = self.logger, file = self.test_csv, dataset_file = {"Key": "test_csv/test.csv",
                                                                          "LastModified": "24/03/21"})
        data = data[0]

        df = pd.DataFrame.from_records(data, columns = self.dq_cols)
        df.drop(["ReportGenerated"], axis = 1, inplace = True)
        self.assertTrue(df.equals(self.test_dq_df))