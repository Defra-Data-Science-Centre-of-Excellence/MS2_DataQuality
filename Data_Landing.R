## Load packages required for Data Quality (DQ) Tool ##
library('readxl') ## Used to ingest Spreadsheets ##
library('dplyr') ## Data Manipulation ##
library('plyr') ## Data Manipulation ##
library(aws.s3) ## Needed to interact with AWS S3 ##
library(aws.ec2metadata) ## Needed to interact with AWS S3 ##

Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')

### GLOBAL SETTINGS ###
## Set up a vector of all eligible column names ##
Colnames <- c("Column", "Null.pct", "One_character", "Data_types", "Percent",
              "Uniqueness", "Contains_geom", "LastModified", "Dataset", "ReportGenerated", "FileExt")

###############
#### LOCAL ####
###############

### Loading in local files (placeholder) ##
do.call_rbind_read.csv <- function(path, pattern = "*.csv") {
  files = list.files(path, pattern, full.names = TRUE)
  do.call(rbind.fill, lapply(files, function(x) read.csv(x, stringsAsFactors = FALSE)))
}

Data <- do.call_rbind_read.csv("./Data")

## Reformat the Date ##
Data$LastModified <- format(as.Date(substr(Data$LastModified, 1, 10), format="%Y-%m-%d"), '%d-%m-%Y')
Data$ReportGenerated <- format(as.Date(substr(Data$ReportGenerated, 1, 10), format="%m/%d/%Y"), '%d-%m-%Y')

## Filter columns based on global setting Metrics ##
## This removes any stray columns and reduces ambiguity ##
Data <- Data %>% dplyr::select(one_of(Colnames))

## Finds data extension ## 
## Data$Dataset_extension <- gsub(".*\\.", "", Data$Dataset)

## Simplify Dataset Name ##
## Assumes data is in a datasetname/data/datasetname.fileextension format 
## e.g. farm_wildlife_package_hotspots/data/refdata_owner.farm_wildlife_package_hotspots.gpkg

## Note, it may be useful to merge in a lookup table between datasets and data dict IDs 
## Data$Dataset <- gsub("/.*", "", Data$Dataset)

###############
##### S3 ######
###############

## Loading in all outputs from AWS S3 ##
## This will append all datasets in one go ## 
## TBU ## 