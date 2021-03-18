from dataHandlers import create_gpkg_metadata, create_gpkg_data_quality_report
from unittest import TestCase
import os


class testGPKGHandler(TestCase):
    """
    Testing for the functions in the gpkg_handling package
    """
    def test_create_gpkg_metadata(self):
        """
        Test that the create_csv_metadata_function outputs the list of layers, headers and number of rows
        :return:
        """
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\testData\\test.gpkg", "rb") as f:
            test_gpkg = f.read()

        layers, headers_list, num_rows = create_gpkg_metadata(file = test_gpkg)
        self.assertEqual(layers, ['test'])
        self.assertEqual(headers_list, [['City', 'Country', 'Latitude', 'Longitude', 'geometry']])
        self.assertEqual(num_rows, [5])

    def create_gpkg_data_quality_report(self):
        # TODO define test
        pass
