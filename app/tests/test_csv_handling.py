from unittest import TestCase
import os
from io import StringIO
import csv
from dataHandlers import csv_handling


class testCsvHandling(TestCase):
    """
    Testing for the functions in the csv_handling package
    """
    def test_create_csv_metadata(self):
        """
        Test that the create_csv_metadata_function outputs the list of headers and the CSV length
        :return:
        """
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\testData\\test.csv") as f:
            test_csv = f.readlines()
        header_list, num_rows = csv_handling.create_csv_metadata(test_csv)
        self.assertEqual(header_list, ['one', 'two', 'three'])
        self.assertEqual(num_rows, 3)

    def test_create_csv_data_quality_report(self):
        # TODO define test
        pass
