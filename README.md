# Metadata Automation Script

This script is designed to automate the creation of a metadata file on the local system the script is run from, and to a specified AWS S3 Bucket that the user has **WRITE** permissions to.

## Table of Contents
[User Guide](#userguide)
1. [Prerequisites](#user-prereqs)
2. [Config File](#user-config)
3. [Calling the Script on the CLI](#user-script)
4. [Viewing the Metadata File Output](#user-output)
5. [Reviewing Logs](#user-logs)

[Data Loading Guide](#dataloading)
1. [Root Directory](#dataload-rd)
2. [Dataset Sub-Directories](#dataload-sd)
3. [Manifest Files](#dataload-mf)

<a name="userguide"></a>
## User Guide
This element of the readme describes how a user should engage with this script, including installing dependencies, configuration, and calling from the command line.

<a name="user-prereqs"></a>
### 1. Prerequisites
#### 1.1 Scientific Computing Environment (SCE) Permissions
The script is designed to be run from 'Ranch' virtual machines on the SCE, linking to the 'Ranch' AWS S3 buckets.
As such, your will need to give the Virtual Machine you are executing the script from the following permissions:
- READ permissions to the buckets you are running the crawler over
- WRITE permissions to the bucket you are writing the metadata file back to

This is easily requested on the SCE Slack **#requests** channel asking to grant your virtual machine (e.g. **ranch-000**) access to the s3 buckets (e.g. **s3-ranch-000**) via an **EC2 role**.

#### 1.2 S3 Folder Structure
The script relies on a certain file structure being in place on each S3 bucket in order to scan it effectively. **NOTE** that data and files outside this structure will not be picked up by the crawler script.

Please see the **"Loading Data Guide"** section below for specifics on this.

#### 1.3 Python Version
This script uses Python 3, in addition to a number of dependencies that are installed with the Python package manager, PIP.
First, check to ensure that Python is installed, and working on your command line.
```
python3 --version
```
If this returns an error, try:
```
python --version
```
The CLI should then return along the lines of:
```
Python 3.x.x
```
The script needs a minimum of Python 3.7 to run, if the python version currently installed is older than this, please install using the [following guidance](https://docs.python-guide.org/starting/installation/).


#### 1.4 Installing Dependencies
To install dependencies, the Python package manager PIP will be needed.
If `pip` is not installed, please install it using the below command.

**NOTE** you may require elevated permissions to do this (`sudo`), and as such will be asked for your password.
 ```
sudo apt install python3-pip
```
Navigate into the repository main folder 'elmsMetadata' and enter the following:
```
pip3 install -r requirements.txt
```
The requirements should take about 5 mins to install.
The packages `fiona` and `geopandas` are required for parsing the Geospatial data. If there is any difficulty in installing these, please refer to the guide [here](https://geopandas.org/getting_started/install.html).

<a name="user-config"></a>
### 2. Config File
The config file is necessary for running the script, and is helpful for allowing users to be able to change the storage and credential configurations with ease.
The config file contains the following information:
- Buckets to scan and return metadata for
- Bucket location to export the metadata file to
- File path for credentials on the machine you run the script from

A template with example values for the config file is included in this repository: `configExample.json` located in the base repository directory.

The config file should look as follows:
```
{
  "buckets_to_read": ["s3-ranch-020", "s3-staging-area"],
  "bucket_to_write_to": "s3-staging-area",
  "metadata_file_name": "elms_metadata",
  "metadata_destination_directory" : "metadata_output",
  "dq_file_name" : "data_quality_output",
  "dq_destination_directory": "data_quality_output"
}
```
The following table details what these fields are used for in the scripts:

| Field Name in Config | Description/Usage | Data Type | Example Value(s) |
|---|---|---|---|
| buckets_to_read | The names of the S3 buckets to scan and pull metadata/DQ from | Array of Strings | [“your-bucket-1”, “your-bucket-2”] |
| buckets_to_write_to | The name of the bucket to write metadata/DQ outputs to | String | “s3-staging-area” |
| metadata_file_name | The name of the output metadata file. *NOTE* - The script automatically adds the ‘.csv’ extension upon export, so this does not need to be in this name | String | “elms_metadata” |
| metadata_destination_directory | A folder within the target S3 bucket to write the file to. Recommended to keep these in the same place after first creation | String | “folder/within/root/dir” *or* “my_folder_name” |
| dq_file_name | The name of the output DQ  file. NOTE - The script automatically adds the ‘.csv’ extension upon export, so this does not need to be in this name | String | “data_quality_output” |
| dq_destination_directory | A folder within the target S3 bucket to write the file to | String | “data_quality_output” |

<a name="user-script"></a>
### 3. Calling the Script on the CLI
When running the script, the user must specify the mode they want the script to run (data quality or metadata), in addition to giving the script the location of the metadata file.
Call the script with the following template:
```
python3 elmMetadataDQTool.py [MODE] [CONFIG PATH]
```
The mode and the config path must be entered in the order as above.

**NOTE** that if your python installation responded to `python --version` instead of `python3 --version` please substitute `python3` for `python` in the command. Whether this is needed depends on how python was installed on the machine.

##### For MODE, please enter one of the following:
- `dq`
- `metadata`

##### For CONFIG PATH, enter an absolute file path to the config file:
Example: `C://path/to/config.json`

An example of using the metadata mode would be:
```
python3 elmMetadataDQTool.py "metadata" "C://path/to/my/config.json"
```
And the DQ example:
```
python3 elmMetadataDQTool.py "dq" "C://path/to/my/config.json"
```

<a name="user-output"></a>
### 4. Viewing the Metadata File Output
The metadata file is exported as a CSV file into the `outputs` subdirectory. The file is also exported to the S3 location you specified in the Config File.

<a name="user-logs"></a>
### 5. Reviewing Logs (in case of error)
The command line tool writes logs to the local file system for debug purposes. This log is to keep a track of the processes that take place inside the script while it is accessing cloud storage, and will provide detailed information on failures.

Logs are written in .txt format and timestamped to the created `logs` folder within this directory.


<a name="dataloading"></a>
## Loading Data Guide (S3 File Structure)
The metadata solution relies on a certain folder structure and supplementary files being in place in order to populate the metadata file effectively.

<a name="dataload-rd"></a>
### 1. The Root Directory
Each S3 bucket is given a root address (e.g. `s3://my-bucket-1/`) from which files and folders can be further added. The script is designed to read every folder (but not file) within this base directory.
Therefore, each dataset should be given its own folder in the root directory:

:mailbox_with_no_mail: my-bucket-1  
 ┣ :open_file_folder: dataset1  
 ┣ :open_file_folder: dataset2  
 ┗ :open_file_folder: dataset3

<a name="dataload-sd"></a>
### 2. The Sub-Directories
Within each dataset folder, the following folders **MUST** exist:
- `data`
- `manifest`
- `data_dictionary` (although this folder will not affect the performance of the script)

The actual dataset should sit within the `data` folder (if there are multiple files like shape files, the structure should be flat, all files within the data folder, with no further sub-directories)  
**Please note** - If there are two file types in the same dataset, first check that one does not compliment the other (i.e. is a schema for the larger file). If they are separate datasets in the same bundle, they should be separated into separate datasets with separate manifest files.

The `manifest` folder should contain only the dataset's `manifest.json` (the file should always be named this). This file is important for the script to pick up information on the dataset relating to uses for modelling and should not be omitted.

The `data_dictionary` is a folder for storing the data dictionary for the dataset. The script does not read this directory, but it is helpful to store this information in a uniform place for each dataset.

Your dataset directory should look like the following:

:open_file_folder: dataset1  
 ┣ :open_file_folder: data  
 ┃ ┗ :page_facing_up: my_example_data.csv  
 ┣ :open_file_folder: manifest  
 ┃ ┗ :page_facing_up: manifest.json  
 ┣ :open_file_folder: data_dictionary  
 ┃ ┗ :page_facing_up: data_dictionary_example.xlsx


<a name="dataload-mf"></a>
### 3. The Manifest Files (manifest.json)
The manifest files are key for the script to incorporate ELM-specific information into the metadata file. As mentioned above, this file must be added into the `manifest` directory within each dataset's folder, and must **ALWAYS** be named `manifest.json`.

An example of the manifest file is given within the `manifestTemplates` directory in this repository, but here we cover all the fields within the document.
In the below block featuring an example of the document.

```
{
  "data_name": "string",
  "generated_by_elm": "string - pick list",
  "data_description": "string",
  "responsible_organisation": "string",
  "data_sensitivity": "string",
  "sharing_agreements_or_copyright_limitations": "string",
  "public_access_limitations": "string",
  "data_type": "string",
  "spatial_reference_system": "string",
  "data_acquired_by_staff": "string",
  "data_acquired_method": "string",
  "data_acquired_date": "datetime",
  "elm_contact": "string",
  "data_manager": "string",
  "data_custodian": "string",
  "eo_clean_air": "boolean",
  "eo_clean_water": "boolean",
  "eo_plants_wildlife": "boolean",
  "eo_bhe": "boolean",
  "eo_hazards": "boolean",
  "eo_climate_change": "boolean",
  "ms3_uptake": "boolean",
  "ms4_environmental_value": "boolean",
  "ms5_payments": "boolean",
  "ms6_delivery_rate": "boolean"
}
```

Most of the field names are self explanatory, however a number require special instruction for use.

**Fields to be aware of:**
- **generated_by_elm** - If data is made by ELM, indicate purpose towards modelling. This value should be chosen from the following: Input, Output, Temp, Survey, Other collection.
- **responsible_organisation** - The organisation the data originated from (i.e. Rural Payments Agency).
- **data_sensitivity** - Give the Sensitivity of the data (i.e. Official, Official Sensitive, Secret, Top Secret)
- **sharing_agreements_or_copyright_limitations** - List any restrictions or contracts pertaining to data access.
- **public_access_limitations** - List any restrictions or contracts that affect public access.
- **data_type** - Indicate nature of the data (i.e. Tabular, Geospatial, Image, etc.)
- **data_acquired_date** - Date should be in the format **YYYY-MM-DD**
- **'eo' fields** - True/False fields for if the data is relevant to Environment Objectives.
- **'ms' fields** - True/False fields for if the data is relevant to certain models.

A full guide to all the fields in the manifest file is available in the Low-Level Design document.
