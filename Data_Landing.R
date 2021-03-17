
## Load packages required for Data Quality (DQ) Tool ##
library('readxl') ## Used to ingest Spreadsheets ##
library('dplyr') ## Data Manipulation ##
library(aws.s3) ## Needed to interact with AWS S3 ##
library(aws.ec2metadata) ## Needed to interact with AWS S3 ##

Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')

### GLOBAL SETTINGS ###
## How many metrics are in the DQ tool (excluding dataset name and column name)? ##
Metrics = 6 

### Loading in local files (placeholder) ##
Data <- read_excel("Data_quality_1_snapshot_17.3.2021.xlsx")

## Filter columns based on global setting Metrics ##
## This removes any stray columns and reduces ambiguity ##
Data <- Data[,c(1:(Metrics+2))] ## Number of metrics + 2 cols for dataset name and column name 

## Finds data extension ## 
Data$Dataset_extension <- gsub(".*\\.", "", Data$Dataset)

## Simplify Dataset Name ##
## Assumes data is in a datasetname/data/datasetname.fileextension format 
## e.g. farm_wildlife_package_hotspots/data/refdata_owner.farm_wildlife_package_hotspots.gpkg

## Note, it may be useful to merge in a lookup table between datasets and data dict IDs 
Data$Dataset <- gsub("/.*", "", Data$Dataset)

## Loading in all outputs from AWS S3 ## 
## TBU ## 