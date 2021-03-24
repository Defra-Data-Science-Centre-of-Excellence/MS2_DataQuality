from scripts.Crawler.CloudDataStorageManager import ShapeFileCollator
from unittest import TestCase
from uuid import uuid4
import os
import shutil


class testShapeFileCollator(TestCase):
    """
    Testing for the ShapeFileCollator
    """

    def setUp(self) -> None:
        self.sfc = ShapeFileCollator(dataset_dir = "test_dir")

    def test_add_file(self):
        """
        Test that the create_shape_metadata outputs the headers and number of rows
        :return:
        """
        test_extension = [".dbf",
                          ".prj",
                          ".shp",
                          ".shx"]

        for extension in test_extension:
            self.sfc.add_file(file = bytes(uuid4().hex, encoding = "utf-8"), file_extension = extension,
                              current_dir = "test_dir", file_size = "1000")
            self.assertNotEqual(getattr(self.sfc, f"_{extension.strip('.')}"), None)

        try:
            irrelevant_extension = ".csv"
            self.sfc.add_file(file = bytes(uuid4().hex, encoding = "utf-8"), file_extension = irrelevant_extension,
                              current_dir = "test_dir", file_size = "1000")
        except Exception as e:
            print(e)
            self.fail("add_file failed when passing an irrelevant file extension")

        test_extension = test_extension[0]

        self.assertRaises(ValueError,
                          self.sfc.add_file,
                          bytes(uuid4().hex, encoding = "utf-8"),
                          test_extension[0],
                          "a_different_dir"
                          )

    def test_is_complete(self):
        """

        :return:
        """

        test_extension = [".dbf",
                          ".prj",
                          ".shp",
                          ".shx"]

        for extension in test_extension:
            self.sfc.add_file(file = bytes(uuid4().hex, encoding = "utf-8"), file_extension = extension,
                              current_dir = "test_dir", file_size = "1000")
        self.assertEqual(self.sfc.is_complete(), True)

    def test_zip_complete_file(self):

        test_extension = [".dbf",
                          ".prj",
                          ".shp",
                          ".shx"]

        for extension in test_extension:
            self.sfc.add_file(file = bytes(uuid4().hex, encoding = "utf-8"), file_extension = extension,
                              current_dir = "test_dir", file_size = "1000")

        output_loc, file_size = self.sfc.zip_complete_file()

        self.assertEqual(os.path.exists(output_loc), True)
        shutil.rmtree(output_loc)

        self.assertEqual(file_size, "1000")
