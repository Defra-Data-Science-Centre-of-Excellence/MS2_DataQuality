## Libraries ##
import os
from os.path import splitext
import pandas as pd
from datetime import datetime
import boto3
import numpy as np

import app.main_aux as main_aux
logger = main_aux.create_logger()


## Functions and Classes ## 
from dataHandlers import *
import app.main_aux
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
    file_in_memory = cdsm.read_file_from_storage(bucket=bucket, key = file['Key']) ## Returns as bytes
    
    _, dataset_file_extension = splitext(file["Key"])
    print(dataset_file_extension)
    
    if dataset_file_extension=='.gpkg':
        print('got.gpkg')
        create_gpkg_data_quality_report(file = file_in_memory) 
        gdf_list = create_geospatial_metadata_and_dq(file_in_memory, type = "gpkg", output = "dq")
        
        print(gdf_list) ## 
        
        for gdflist in gdf_list:
            #print(gdflist)
            #print(gdflist.describe()) ## Prints simple summary tables 
            
            Length = len(gdflist)
            
            ## Convert Blank data into NA ## 
            gdflist.replace(r'', np.NaN, inplace=True)
            
            ## Uniqueness ##
            ### How many rows are unique? ###
            print("How many rows are unique?")
            if Length > 0:
                
                try:
                    Full_unique = 1 - (len(gdflist)-len(gdflist.drop_duplicates()))/Length
                    print(Full_unique)
            
                except Exception as e:
                    print(e)
                    print(f"ERROR: {gdflist} does not contain any rows")

            ## Note, we could attempt a 'partial' duplication match here, but this would be computationally intensive
            ## For example, checking for duplication over 50% of columns = (n)!/0.5n!(n-0.5n)! --> n!/((0.5n)!)^2 --> grows exponentially 
            
            ## Completeness ##
            gdflist_df = pd.DataFrame(gdflist) ## Converts from geodf to normal pandas df 
            
            Colnames = list(gdflist_df)
            print("Printing column names...")
            print(Colnames)
            
            Geom_check = [item for item in Colnames if item.startswith('geom')]
            
            if len(Geom_check)==1:
               Contains_geom = 'Yes'
            else:
               Contains_geom = 'No'
             
            print(Contains_geom)
       
            print('Checking number of NA entries per column')
            #gdflist_df['X'] = np.NaN -- Dummy example of a column which is all NA 
            NA_entries = pd.DataFrame((gdflist_df.isnull().sum(axis = 0)/Length), columns=['NA_pct'])
            
            NA_entries.index.name = 'Column'
            NA_entries.reset_index(inplace=True)
            
            print(NA_entries)
            print(NA_entries.shape)
            
            
            print('Checking number of entries per column which only contain one character (excuding alphanumeric entries)')
            ## For now we will just check for four particular string entries, these can be expanded over time 
            ## Note, there may be a more elegant regex solution to this 
            
            #gdflist_df['X'] = '.' ## Dummy example of a column which is all =. 
            #gdflist_df['Y'] = '-' ## Dummy example of a column which is all =. 
            
            Char_1 = list()
            
            for col in gdflist_df:
                Boolean = ((gdflist_df[col] == '.')|(gdflist_df[col] == ',')|(gdflist_df[col] == '-')|(gdflist_df[col] == '')).all()
                Char_1.append([col, Boolean])
                
            Char_1 = pd.DataFrame(Char_1, columns = ['Column', 'One_character'])
            Char_1['One_character'] = Char_1['One_character'].astype(str)
            
            Char_1['One_character'] = np.where(Char_1['One_character']=='False', 
                                              'Contains at least one alphanumeric character', 
                                              'Does not contain any alphanumeric characters')
            
            print(Char_1)
            print(Char_1.shape)
                
            ## Validity ## 
            #gdflist_df['X'] = 1
            #gdflist_df['X'] = gdflist_df['X'].astype(str)
            #gdflist_df.loc[0, 'X'] = 'String'
            #gdflist_df.loc[1, 'X'] = np.nan

            
            Data_types = pd.DataFrame(gdflist_df.dtypes, columns = ['Data_types'])
            
            print(Data_types)
            print(Data_types.shape)
            
            print('Loop over columns which are in object format and check if they contain at least one non-numeric entry')
            ## https://pbpython.com/pandas_dtypes.html ##
            
            Columns_to_iterate = Data_types.index[(Data_types['Data_types'] == 'object')].tolist()
            
            print(Columns_to_iterate)
            
            Object_type_checker = []
            
            try:
                ## If length > 1, then the loop is feasible 
                if len(Columns_to_iterate) > 1:  
                    for col in Columns_to_iterate:
                        print(col)
                        
                        Temp = gdflist_df[gdflist_df[col].notna()] ## Removes rows which are NA 
                        Temp['checker'] = Temp[col].str.match("\d+") ## TRUE = Only Digits, FALSE = Anything else
                        print(Temp['checker'])
                        
                        ## If Pct = 1, then the object should be a float/integer 
                        ## Note, we are using len(Temp[checker]) and not Length, as the removal of NAs may generate a difference between the two
                        Pct = Temp['checker'].sum()/len(Temp['checker'])
                        
                        Object_type_checker.append([col, Pct])
                        
                Object_type_checker = pd.DataFrame(Object_type_checker, columns = ['Column', 'Percent']) 
                
                ## If Pct = 1, then the column should be numeric ##
                ## If Pct < 1, it is appropriately imported as an object ##
                Object_type_checker['Percent'] = np.where(Object_type_checker['Percent']==1, 
                                                          'Data type mismatch: should be converted to numeric', 
                                                          'Data type is set correctly')
                
                print(Object_type_checker)
                    
            except Exception as e:
                print(e)
                print(f"NOTE: Dataframe does not contain any columns of object type")
                    

            ## Combine and consolidate all outputs into one single dataframe ##
                """
                Dataframes and values to collate into one output DF:
                Uniqueness: Full_unique
                Completeness: Contains_geom, NA_entries, Char_1
                Validity: Data_types, Object_type_checker
                
                Join NA_entries, Char_1, Data_types and Object_type_checker together 
                Add additional columns for Full_unique and Contains_geom 
                
                """
                
            #def Merge(df_to_merge, output):
            #    outputdf = output.merge(df_to_merge, how='left', left_on='Column', right_on='Column')
            #    return outputdf
            
            ### Reset Index in Data Types before Merge ## 
            Data_types.index.name = 'Column'
            Data_types.reset_index(inplace=True)
            
            ## One liner merge ## 
            ## Alternative to using the function 'Merge'
            Output_dataframe = NA_entries.merge(Char_1, how='outer', on='Column').merge(Data_types, how='outer', on='Column').merge(Object_type_checker, how='outer', on = 'Column')
            
            ## Fill NA values in Percent Column with the appropriate string (data already appropriately encoded as int64 or float64)
            Output_dataframe['Percent'].fillna('Data type is set correctly', inplace=True)
            
            ## These outputs are the same across all rows, but we can handle these in the Shiny dashboard ## 
            Output_dataframe['Uniqueness'] = Full_unique
            Output_dataframe['Contains_geom'] = Contains_geom
            
            print(Output_dataframe)
            print(Output_dataframe.shape)
            
            ## Testing to make sure no stray columns have been inserted in the DQ Process ## 
            if len(Output_dataframe) == len(list(gdflist_df)):
                print('Writing to CSV')
                print("Need to write this to S3 in the next iteration...")
                
                Output_dataframe.to_csv('Data_quality_1.csv', index = False)
                
            else:
                print(f"ERROR: The output dataframe does not have the same number of columns as the original input file")
            
    else: 
        pass
    


print('Done')
