# import main_aux
#os.chdir('C:\\Users\\hassethi\\OneDrive - Capgemini\\Documents\\Defra\\DQ_Metadata_Build\\elmsMetadata\\app')
## Libraries ##
import os
import pandas as pd
from datetime import datetime
import boto3

## Functions and Classes ## 
from dataHandlers import *
from main_aux import *
from Crawler import crawler

## Import Credentials ##
credentials = load_json_file("C:\\Users\\hassethi\\OneDrive - Capgemini\\Documents\\Defra\\DQ_Metadata_Build\\aws.json")

## Initialise Boto3 ##
S3client = boto3.client('s3', 
             aws_access_key_id=credentials["aws_access_key_id"], 
             aws_secret_access_key=credentials["aws_secret_access_key"])

## https://boto3.amazonaws.com/v1/documentation/api/latest/index.html ## 
## Use this documentation ##

Objects = S3client.get_paginator('list_objects')

## Look at shape data handlers (Ben E's code)
## Shapefile --> Memory -> Zip --> Geopandas 
