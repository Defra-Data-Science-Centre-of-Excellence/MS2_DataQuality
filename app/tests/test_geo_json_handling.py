from app.dataHandlers import create_geojson_metadata, create_geojson_data_quality_report
from unittest import TestCase
import os


class testGEOJsonHandler(TestCase):
    """
    Testing for the functions in the geojson_handing package
    """

    def test_create_geojson_metadata(self):
        """
        Test that the create_csv_metadata_function outputs the list of layers, headers and number of rows
        :return:
        """
        with open(f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\testData\\test.json", "rb") as f:
            test_geojson = f.read()

        header_list, num_rows = create_geojson_metadata(file = test_geojson)
        print(header_list)
        print(num_rows)
        self.assertEqual(header_list, ['OBJECTID', 'name', 'opened', 'start', 'end_', 'length_km', 'length_mil',
                                       'updated', 'last_vr', 'Shape_Length', 'geometry'])
        self.assertEqual(num_rows, 4)

    def create_geojson_data_quality_report(self):
        # TODO define test
        pass
