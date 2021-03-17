


def generate_data_quality_output_files(bucket: str):

    ## Based on an abstract based cloud (ABC)
    ## This implicitly uses the load_json_file function
    cdsm = CloudDataStorageManagerAWS(credentials_fp = AWS_credentials) ## From Crawler.CloudDataStorageManager.cloud_data_storage_manager
    all_dataset_file = cdsm.get_dataset_files_list(bucket = bucket) ## From Crawler.CloudDataStorageManager.cloud_data_storage_manager

    for file in all_dataset_file:
        file_in_memory = cdsm.read_file_from_storage(bucket=bucket, key = file['Key']) ## Returns as bytes

        _, dataset_file_extension = splitext(file["Key"])
        print(dataset_file_extension)

        if dataset_file_extension=='.gpkg':
            print('Generating report for .gpkg geospatial data')
            create_gpkg_data_quality_report(file = file_in_memory) ## From dataHandlers (varies by geospatial data type)
            gdf_list = create_geospatial_metadata_and_dq(file_in_memory, type = "gpkg", output = "dq") ## From geospatial_handling.py

            for gdflist in gdf_list:
                ## Used in later operations ##
                Length = len(gdflist)

                ## Convert Blank data into NA ##
                gdflist.replace(r'', np.NaN, inplace=True)

                ## Uniqueness ##
                ### How many rows are unique? ###
                print("Checking for row-wise duplication...")
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
                print("Checking for row-wise duplication...")
                Geom_check = [item for item in Colnames if item.startswith('geom')]

                if len(Geom_check)==1:
                   Contains_geom = 'Yes'
                else:
                   Contains_geom = 'No'

                print('Checking number of NA entries per column')
                #gdflist_df['X'] = np.NaN -- Dummy example of a column which is all NA
                NA_entries = pd.DataFrame((gdflist_df.isnull().sum(axis = 0)/Length), columns=['NA_pct'])

                NA_entries.index.name = 'Column'
                NA_entries.reset_index(inplace=True)

                print('Checking number of entries per column which only contain one character (excuding alphanumeric entries)')
                ## For now we will just check for four particular string entries, these can be expanded over time
                ## Note, there may be a more elegant regex solution to this

                Char_1 = list()

                for col in gdflist_df:
                    Boolean = ((gdflist_df[col] == '.')|(gdflist_df[col] == ',')|(gdflist_df[col] == '-')|(gdflist_df[col] == '')).all()
                    Char_1.append([col, Boolean])

                Char_1 = pd.DataFrame(Char_1, columns = ['Column', 'One_character'])
                Char_1['One_character'] = Char_1['One_character'].astype(str)

                Char_1['One_character'] = np.where(Char_1['One_character']=='False',
                                                  'Contains at least one alphanumeric character',
                                                  'Does not contain any alphanumeric characters')


                ## Validity ##
                print('Output the data type for each column in the geodataframe')
                ## https://pbpython.com/pandas_dtypes.html ##
                Data_types = pd.DataFrame(gdflist_df.dtypes, columns = ['Data_types'])

                print('Loop over columns which are in object format and check if they contain at least one non-numeric entry')
                Columns_to_iterate = Data_types.index[(Data_types['Data_types'] == 'object')].tolist()

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

                except Exception as e:
                    print(e)
                    print(f"NOTE: Dataframe does not contain any columns of object type")

                ## Combine and consolidate all outputs into one single dataframe ##
                print("Combine and consolidate all data quality outputs into a single dataframe")
                    """
                    Dataframes and values to collate into one output DF:
                    Uniqueness: Full_unique
                    Completeness: Contains_geom, NA_entries, Char_1
                    Validity: Data_types, Object_type_checker

                    Join NA_entries, Char_1, Data_types and Object_type_checker together
                    Add additional columns for Full_unique and Contains_geom

                    """

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
                print("Dimensions of the data quality report (rows * columns...)")
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

        return Output_dataframe

Output_dataframe = generate_data_quality_output_files(bucket = "elms-test-2") ## ELMS test bucket
