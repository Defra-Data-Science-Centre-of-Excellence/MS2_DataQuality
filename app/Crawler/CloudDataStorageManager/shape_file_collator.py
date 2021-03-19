import zipfile
import io
import uuid
import os


class ShapeFileCollator(object):
    """
    Class used to keep relevant shape format constituent files in memory until they can be zipped to be loaded
    """

    def __init__(self, dataset_dir: str):
        """
        Constructor

        Used to:
        - set up private vars to keep shape format constituent files in memory
        :param dataset_dir: name of the dataset the shape file collector is working on
        """
        self.name = dataset_dir
        self._dbf = None
        self._prj = None
        self._shp = None
        self._shx = None
        self._shp_file_size = None
        self._attrs = [self._shp, self._prj, self._dbf, self._shx]

    def add_file(self, file: bytes, file_extension: str, current_dir: str, file_size: str) -> None:
        """
        Allocate a file to
        :param file_size:
        :param file: file in bytes
        :param file_extension: extension of the file
        :param current_dir: the dataset directory the file comes from
        :param file_size: size of the file in bytes
        :return: nothing
        """
        if self.name != current_dir:
            # this is here to throw an error if we start processing multiple datasets at once
            raise ValueError("Data isnt't being passed to the collator in the expected fashion aborting")
        else:
            if file_extension == ".dbf":
                self._dbf = file
            elif file_extension == ".prj":
                self._prj = file
            elif file_extension == ".shp":
                self._shp = file
                self._shp_file_size = file_size
            elif file_extension == ".shx":
                self._shx = file
            else:
                pass

    def is_complete(self) -> bool:
        """
        Check if the class has collated every constituent file required to load shape file into memory
        :return: bool
        """
        self._attrs = [self._shp, self._prj, self._dbf, self._shx]
        if any(attr is None for attr in self._attrs):
            return False
        else:
            return True

    def zip_complete_file(self) -> tuple:
        """
        Zips up all files in private vars, dumps to temp directory on disk
        :return: disk location of zip
        """

        uuid_to_use = uuid.uuid4().hex
        zip_file_output_name = f"{uuid_to_use}.zip"
        zip_buffer = io.BytesIO()

        files = [(f'{zip_file_output_name}.dbf', io.BytesIO(self._dbf)),
                 (f'{zip_file_output_name}.prj', io.BytesIO(self._prj)),
                 (f'{zip_file_output_name}.shp', io.BytesIO(self._shp)),
                 (f'{zip_file_output_name}.shx', io.BytesIO(self._shx))]

        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            for file_name, data in files:
                zip_file.writestr(file_name, data.getvalue())
        zip_file_output_dir =  f'{os.getcwd()}/temp'

        if not os.path.exists(zip_file_output_dir):
            os.makedirs(zip_file_output_dir)

        zip_file_output_loc = f'{zip_file_output_dir}/{zip_file_output_name}'

        with open(zip_file_output_loc, 'wb') as f:
            f.write(zip_buffer.getvalue())

        return zip_file_output_loc, self._shp_file_size

    def __str__(self):
        return "ShapeFileCollator object"

    def __repr__(self):
        return self.__str__()
