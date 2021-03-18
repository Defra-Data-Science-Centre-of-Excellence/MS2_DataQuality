"""
Assumptions
- only supports a dataset with one geometry column
"""

import numpy as np
import pandas as pd
from os.path import dirname, splitext
from datetime import datetime

# silence SettingWithCopyWarning warning
pd.set_option('mode.chained_assignment', None)


def create_dq_reports(gdf_list: list, file_dict: dict) -> list:
    """
    Create data quality reports for a list of geodataframes
    :param gdf_list: list of geodataframes
    :param file_dict:
    :return: list - of dataframes that are dq reports
    """
    gdf_dq_reports = []

    for gdf in gdf_list:

        len_gdf = len(gdf)

        gdf.replace(r'', np.NaN, inplace=True)

        # ASSESSMENT OF UNIQUENESS
        if len_gdf > 0: # BE: why do we need to check if the length is > 0?
            try:
                full_unique = 1 - (len(gdf) - len(gdf.drop_duplicates())) / len_gdf
            except Exception as e:
                # BE: I don't think this is necessary
                print(e)
                print(f"ERROR: gdf does not contain any rows")

        # HS: Note, we could attempt a 'partial' duplication match here, but this would be computationally intensive
        # HS: For example, checking for duplication over 50% of columns = (n)!/0.5n!(n-0.5n)! --> n!/((0.5n)!)^2 -->
        # grows exponentially

        # ASSESSMENT OF COMPLETENESS
        gdf_as_df = pd.DataFrame(gdf)
        # BE: is is necessary to convert to pd df, do gdfs not have the same functionality?
        # col_names = [col for col in gdf_as_df.columns]
        # BE: we can get geom_cols based on dtypes, much better than dealing with col name
        geom_cols = list(gdf.select_dtypes(include=['geometry']).columns)

        if geom_cols:
            contains_geom = True
            observed_geom_types = None

            # BE: loop through every geometry column, not expecting more than one
            for col in geom_cols:
                # BE: get a list of every entry in col
                geoms = gdf[col].tolist()
                # BE: get the type of every entry (i.e. Polygon, Multipolygon) -> get the set of this
                geom_types = set([type(g).__name__ for g in geoms])
                # BE: create a comma seperated string of the geom types observed
                geom_types = ",".join(geom_types)
                observed_geom_types = geom_types
                # BE: check that every geom is valid
                try:
                    geom_valid_check = [obj.is_valid for obj in geoms]
                except Exception as e:
                    print(e)
                    geom_valid_check = [False for obj in geoms]

                invalid = [str(i) for i, j in enumerate(geom_valid_check) if j is False]


        else:
            contains_geom = False

        # HS: check for nans in columns
        na_entries = pd.DataFrame((gdf_as_df.isnull().sum(axis = 0) * 100/ len_gdf), columns = ['Null pct'])

        na_entries.index.name = 'Column'
        na_entries.reset_index(inplace = True)

        # HS: For now we will just check for four particular string entries, these can be expanded over time
        # HS: Note, there may be a more elegant regex solution to this

        # BE: I think we could find a nicer way to do this but let's keep this for now
        # BE: not sure what "char_1" means?
        # BE: if all of this is check if object cols are str or if they're object interpreted numbers
        # this might be nicer as it's own function that's documented
        char_1 = []

        for col in gdf_as_df:
            col_check = ((gdf_as_df[col] == '.') | (gdf_as_df[col] == ',') | (gdf_as_df[col] == '-') | (
                        gdf_as_df[col] == '')).all()
            # BE: I refactored here to col check, was "Boolean" before. Should avoid use of basic dtypes as var names
            char_1.append([col, col_check])

        char_1 = pd.DataFrame(char_1, columns = ['Column', 'One_character'])
        char_1['One_character'] = char_1['One_character'].astype(str)

        char_1['One_character'] = np.where(char_1['One_character'] == 'False',
                                           'Contains at least one alphanumeric character',
                                           'Does not contain any alphanumeric characters')

        interpreted_dtypes = pd.DataFrame(gdf_as_df.dtypes, columns = ['Data_types'])

        object_cols = interpreted_dtypes.index[(interpreted_dtypes['Data_types'] == 'object')].tolist()

        object_type_checker = []

        try:

            for col in object_cols:
                # BE: what is temp?
                temp = gdf_as_df[gdf_as_df[col].notna()]  # Removes rows which are NA
                temp['checker'] = temp[col].str.match("\d+")  # TRUE = Only Digits, FALSE = Anything else

                # If Pct = 1, then the object should be a float/integer
                # Note, we are using len(Temp[checker]) and not Length, as the removal of NAs may generate a
                # difference between the two
                pct = temp['checker'].sum() / len(temp['checker'])

                object_type_checker.append([col, pct])

            else:
                pass

            object_type_checker = pd.DataFrame(object_type_checker, columns = ['Column', 'Percent'])

            # If Pct = 1, then the column should be numeric
            # If Pct < 1, it is appropriately imported as an object
            object_type_checker['Percent'] = np.where(object_type_checker['Percent'] == 1,
                                                      'Data type mismatch: should be converted to numeric',
                                                      'Data type is set correctly')

        except Exception as e:
            print(e)
            print(f"NOTE: Dataframe does not contain any columns of object type")

        # Reset Index in Data Types before Merge
        interpreted_dtypes.index.name = 'Column'
        interpreted_dtypes.reset_index(inplace = True)

        # One liner merge ##
        # Alternative to using the function 'Merge'
        output_df = na_entries.merge(char_1, how = 'outer', on = 'Column').merge(interpreted_dtypes, how = 'outer',
                                                                                        on = 'Column').merge(
            object_type_checker, how = 'outer', on = 'Column')


        # Fill NA values in Percent Column with the appropriate string (data already appropriately encoded as int64 or
        # float64)
        output_df['Percent'].fillna('Data type is set correctly', inplace = True)

        # These outputs are the same across all rows, but we can handle these in the Shiny dashboard ##
        output_df['Uniqueness'] = full_unique
        output_df['ContainsGeometry'] = contains_geom
        output_df['LastModified'] = file_dict["LastModified"]
        output_df['Dataset'] = dirname(file_dict["Key"]).split("/")[0]
        output_df['ReportGenerated'] = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        output_df["FileExt"] = splitext(file_dict["Key"])[1]

        if contains_geom:
            output_df['GeomTypesObserved'] = observed_geom_types
            if invalid:
                output_df['InvalidGeometriesAtRows'] = ",".join(invalid)
            else:
                output_df['InvalidGeometriesAtRows'] = "None Observed"

        # Testing to make sure no stray columns have been inserted in the DQ Process ##
        if len(output_df) == len(list(gdf_as_df)):
            gdf_dq_reports.append(output_df)
            output_df.to_csv(f"{dirname(file_dict['Key']).replace('/', '_')}.csv", index = False, index_label = False)
        else:
            print(f"ERROR: The output dataframe does not have the same number of columns as the original input file")

    return gdf_dq_reports
