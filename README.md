# Metadata Automation Script

This script is designed to automate the creation of a metadata file on the local system the script is run from, and to a specified AWS S3 Bucket that the user has **WRITE** permissions to.


## User Guide
This element of the readme describes how a user should engage with this script, including installing dependencies, configuration, and calling from the command line.

### 1. Prerequisites
#### Scientific Computing Environment (SCE) Permissions
The script is designed to be run from 'Ranch' virtual machines on the SCE, linking to the 'Ranch' AWS S3 buckets.
As such, your will need to give the Virtual Machine you are executing the script from the following permissions:
- READ permissions to the buckets you are running the crawler over
- WRITE permissions to the bucket you are writing the metadata file back to

This is easily requested on the SCE Slack **#requests** channel asking to grant your virtual machine (e.g. **ranch-000**) access to the s3 buckets (e.g. **s3-ranch-000**) via an **EC2 role**.

#### S3 Folder Structure
The script relies on a certain file structure being in place on each S3 bucket in order to scan it effectively. **NOTE** that data and files outside this structure will not be picked up by the crawler script.

Please see the **"Loading Data"** section below for specifics on this.

#### Python Version
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


#### Installing Dependencies
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

### 2. Config File
The config file is necessary for running the script, and is helpful for allowing users to be able to change the storage and credential configurations with ease.
The config file contains the following information:
- Buckets to scan and return metadata for
- Bucket location to export the metadata file to
- File path for credentials on the machine you run the script from

A template with example values for the config file is included in this repository: `configExample.json` located in the base repository directory.

### 3. Calling the Script on the CLI
When running the script, the user must specify the mode they want the script to run (data quality or metadata), in addition to giving the script the location of the metadata file.
Call the script with the following template:
```
python3 app/main.py [MODE] [CONFIG PATH]
```
The mode and the config path must be entered in the order as above.

**NOTE** that if your python installation responded to `python --version` instead of `python3 --version` please substitute `python3` for `python` in the command. Whether this is needed depends on how python was installed on the machine.

##### For MODE, please enter one of the following:
- `dq`
- `metadata`

##### For CONFIG PATH, enter an absolute file path to the config file:
Example: `C://path/to/config.json`

The resulting command should look like:
```
python3 app/main.py metadata "C://path/to/my/config.json"
```
### 4. Viewing the Metadata File Output
The metadata file is exported as a CSV file into the `outputs` subdirectory. The file is also exported to the S3 location you specified in the Config File.

### 5. Reviewing Logs (in case of error)
The command line tool writes logs to the local file system for debug purposes. This log is to keep a track of the processes that take place inside the script while it is accessing cloud storage, and will provide detailed information on failures.

Logs are written in .txt format and timestamped to the created `logs` folder within this directory.


## Loading Data Guide (S3 File Structure)
The metadata solution relies on a certain folder structure and supplementary files being in place in order to populate the metadata file effectively.

### The Root Directory
Each S3 bucket is given a root address (e.g. `s3://my-bucket-1/`) from which files and folders can be further added. The script is designed to read every folder (but not file) within this base directory.
Therefore, each dataset should be given its own folder in the root directory:

:mailbox_with_no_mail: my-bucket-1  
 ┣ :open_file_folder: dataset1  
 ┣ :open_file_folder: dataset2  
 ┗ :open_file_folder: dataset3

### The Sub-Directories
Within each dataset folder, the following folders **MUST** exist:
- `data`
- `manifest`
- `data_dictionary` (although this folder will not affect the performance of the script)

The actual dataset should sit within the `data` folder (if there are multiple files like shape files, the structure should be flat, all files within the data folder, with no further sub-directories)

The `manifest` folder should contain only the dataset's `manifest.json` (the file should always be named this). This file is important for the script to pick up information on the dataset relating to uses for modelling and should not be omitted.

The `data_dictionary` is a folder for storing the data dictionary for the dataset. The script does not read this directory, but it is helpful to store this information in a uniform place for each dataset.

Your dataset directory should look like the following:

:open_file_folder: dataset1  
 ┣ :open_file_folder: data  
 ┃ ┗ :page_facing_up: my_example_data.csv  
 ┣ :open_file_folder: manifest  
 ┃ ┗ :page_facing_up: manifest.json  
 ┗ :open_file_folder: data_dictionary  
   ┗ :page_facing_up: data_dictionary_example.xlsx


### The Manifest File
The manifest file is key for the script to incorporate ELM-specific information into the metadata file. As mentioned above, this file must be added into the `manifest` directory within each dataset's folder, and must **ALWAYS** be named `manifest.json`.

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
- **'eo' fields** - True/False fields for if the data is relevant to Environment Objectives.
- **'ms' fields** - True/False fields for if the data is relevant to certain models.

## Low Level Design - How the script works
This element of the readme describes the design elements of the script, and the environment (including S3 bucket structure) it is currently configured to use in AWS.

### Prerequisites
AWS Access, key, SCE VM, python as above, Linux

### Access Management Approach
The script is configured to use AWS IAM (in line with the SCE).
AWS key, secret keys, RBAC EC2

### S3 Directory Structure
data, manifest, data_dictionary

### Manifest Files
What they're used for, description of each field

### Crawler Class - Adapter pattern
Cloud agnostic - abstraction for adapting to another Cloud storage provider

### Companion JSON File
Fields, purpose, mapping

### Suggested Features to Implement Later
This section details some technical features that are desired to be implemented or improved in future, but could not be built based on time constraints:
- Trigger-based execution
- Change Data Capture
- Temp file write-out


## Testing
This part of the document includes application test planning and results.

### Unit Tests
Carried out on the code itself to test the underlying logic of functions.
#### Code Coverage
Table, explanations
#### Results
Requirements vs results


### System Integration Testing
Requirements vs results

### User Acceptance Testing
Requirements vs results
