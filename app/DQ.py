## Libraries ##
import os
from os.path import splitext
import pandas as pd
from datetime import datetime
import boto3

## Functions and Classes ## 
from dataHandlers import *
from main_aux import *
from dataHandlers.geospatial_handling import create_geospatial_metadata_and_dq

from Crawler import crawler
from Crawler.CloudDataStorageManager.cloud_data_storage_manager import CloudDataStorageManagerAWS
print(os.getcwd())

## Initialise Boto3 ##
#S3client = boto3.client('s3', 
#             aws_access_key_id=credentials["aws_access_key_id"], 
#             aws_secret_access_key=credentials["aws_secret_access_key"])
## https://boto3.amazonaws.com/v1/documentation/api/latest/index.html ## 

## Import AWS Credentials ##
AWS_credentials = input('Enter JSON file location where the AWS credentials live: ')
credentials = load_json_file(AWS_credentials)
print(AWS_credentials)

## Based on an abstract based cloud (ABC)
## This implicitly uses the load_json_file function 
cdsm = CloudDataStorageManagerAWS(credentials_fp = AWS_credentials)

bucket = "elms-test-2"
all_dataset_file = cdsm.get_dataset_files_list(bucket = bucket)

for file in all_dataset_file:
    #print(file['Key']) 
    file_in_memory = cdsm.read_file_from_storage(bucket=bucket, key = file['Key']) ## Returns as bytes
    
    _, dataset_file_extension = splitext(file["Key"])
    print(dataset_file_extension)
    
    if dataset_file_extension=='.gpkg':
        print('got.gpkg')
        create_gpkg_data_quality_report(file = file_in_memory) 
        gdf_list = create_geospatial_metadata_and_dq(file_in_memory, type = "gpkg", output = "dq")
        
        print(gdf_list) ## 
        
        for gdflist in gdf_list:
            print(gdflist)
            
            ## Code development starts here ##
            ## Use try and error/print exceptions (error handling)
            ## Print e within Exception 
            
            ## Consider output files here ## 
            ## Check if geom column contains ## 
            ## What do we need to test in unit testing? ##
        
    else: 
        pass
    


print('Done')
