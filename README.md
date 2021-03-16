# Metadata Automation Script

This script is designed to automate the creation of a metadata file on the local system the script is run from, and to a specified AWS S3 Bucket that the user has **WRITE** permissions to.


## User Guide
This element of the readme describes how a user should engage with this script, including installing dependencies, configuration, and calling from the command line.

### 1. Prerequisites
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

For MODE, please enter one of the following:
- `dq`
- `metadata`

For CONFIG PATH, enter an absolute file path to the config file you have prepared, for example: `C://path/to/config.json`

The resulting command should look like:
```
python3 app/main.py metadata "C://path/to/my/config.json"
```
### 4. Viewing the Metadata File Output
The metadata file is exported as a CSV file into the `outputs` subdirectory. The file is also exported to the S3 location you specified in the Config File.

### 5. Reviewing Logs (in case of error)
The command line tool writes logs to the local file system for debug purposes. This log is to keep a track of the processes that take place inside the script while it is accessing cloud storage, and will provide detailed information on failures.

Logs are written in .txt format and timestamped to the created `logs` folder within this directory.


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
