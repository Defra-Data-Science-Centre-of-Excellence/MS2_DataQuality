## Load packages required for Data Quality (DQ) Tool ##
library('readxl') ## Used to ingest Spreadsheets ##
library('dplyr') ## Data Manipulation ##
library('plyr') ## Data Manipulation ##
library(aws.s3) ## Needed to interact with AWS S3 ##
library(aws.ec2metadata) ## Needed to interact with AWS S3 ##

Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')

### GLOBAL SETTINGS ###
## Set up a vector of all eligible column names ##
Colnames <- c("Column", "NullPct", "OneCharacter", "DataTypes", "Percent",
              "Uniqueness", "ContainsGeometry", "LastModified", "Dataset", "ReportGenerated", "FileExt",
              "GeomTypesObserved", "InvalidGeometriesAtRows")


##############################
## Ingest Data Quality File ##
##############################

## Function to ingest data from local storage OR the cloud 
Ingest_data_quality_file <- function(storage_class, ...) {
  
  ## If storage_class = 'cloud', then the data will pull from AWS ##
  ## A second conditional argument is denoted by the '...' parameter
  ## This passes the bucket names into the function ## 
  
  if(storage_class=='cloud'){
  
    bucket_name <- c(...)
    
    Data <- aws.s3::s3read_using(
      FUN = read.csv,
      object = 'elmsTestData/data_quality_output/elms_data_quality.csv', # Note: this object path may change in another bucket
      bucket = bucket_name
    )
    
  ## If storage_class = 'local', then the data will pull from local storage ##
  ## This defaults to data saved in a folder called 'Data' on your virtual machine or local machine   
    
  } else if(storage_class=='local') {
    
    do.call_rbind_read.csv <- function(path, pattern = "*.csv") {
      files = list.files(path, pattern, full.names = TRUE)
      do.call(rbind.fill, lapply(files, function(x) read.csv(x, stringsAsFactors = FALSE)))
    
    }
    
    Data <- do.call_rbind_read.csv("./Data/")

  } else{print("Error: Please choose either 'local' or 'cloud'")}
    
  return(Data)
}

## Execute Function ##
Data <- Ingest_data_quality_file('cloud', 's3-staging-area') ## Use this function call for data saved in S3
## Data <- Ingest_data_quality_file('local') <- Use this function call when testing data saved locally 


#############################
## Basic data manipulation ##
#############################

## Reformat the Date ##
Data$LastModified <- format(as.Date(substr(Data$LastModified, 1, 10), format="%Y-%m-%d"), '%d-%m-%Y')
Data$ReportGenerated <- format(as.Date(substr(Data$ReportGenerated, 1, 10), format="%m/%d/%Y"), '%d-%m-%Y')

## Filter columns based on global setting Metrics ##
## This removes any stray columns and reduces ambiguity ##
Data <- Data %>% dplyr::select(one_of(Colnames))