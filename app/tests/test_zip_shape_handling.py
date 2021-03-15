from app.dataHandlers import create_shape_metadata
from unittest import TestCase
import os
import shutil


class testSHPHandler(TestCase):
    """
    Testing for the functions in the zip_shape_handling package
    """

    def test_create_shape_metadata(self):
        """
        Test that the create_shape_metadata outputs the headers and number of rows
        :return:
        """
        # have to set this path up else the test case will delete the test data
        if not os.path.exists("./temp"):
            os.makedirs("./temp")

        test_zip = f"{os.getcwd().split('elmsMetadata')[0]}elmsMetadata\\testData\\test_shape.zip"

        shutil.copyfile(test_zip, "./temp/test_shape.zip")

        header_list, num_rows = create_shape_metadata(file = "./temp/test_shape.zip")

        self.assertEqual(header_list, ['FID', 'Name', 'Opened', 'Start', 'End_', 'Length_Km', 'Length_Mil',
                                       'Updated', 'Last_VR', 'GlobalID', 'SHAPE_Leng', 'geometry'])
        self.assertEqual(num_rows, 13)

        if os.path.exists("./temp"):
            shutil.rmtree("./temp", ignore_errors = True)

    def create_gpkg_data_quality_report(self):
        # TODO define test
        pass
