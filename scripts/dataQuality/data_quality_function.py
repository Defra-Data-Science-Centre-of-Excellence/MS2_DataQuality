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


def create_dq_reports(logger, gdf_list: list, file_dict: dict) -> list:
    """
    Create data quality reports for a list of geodataframes
    :param gdf_list: list of geodataframes
    :param file_dict:
    :return: list - of dataframes that are dq reports
    """
    # TODO add logger
    gdf_dq_reports = []

    for gdf in gdf_list:

        len_gdf = len(gdf)

        gdf.replace(r'', np.NaN, inplace=True)

        # ASSESSMENT OF UNIQUENESS
        if len_gdf > 0: # BE: why do we need to check if the length is > 0?
            try:
                full_unique = 1 - (len(gdf) - len(gdf.drop_duplicates())) / len_gdf
            except Exception as e:
                print(e)
                logger.debug(e)
                logger.debug("ERROR: gdf does not contain any rows")

        # ASSESSMENT OF COMPLETENESS
        gdf_as_df = pd.DataFrame(gdf)
        geom_cols = list(gdf.select_dtypes(include=['geometry']).columns)

        if geom_cols:
            contains_geom = True
            observed_geom_types = None
            count = 0
            # BE: loop through every geometry column, not expecting more than one
            for col in geom_cols:

                # BE: get a list of every entry in col
                geoms = gdf[col].tolist()
                # BE: get the type of every entry (i.e. Polygon, Multipolygon) -> get the set of this
                geom_types = set([type(g).__name__ for g in geoms])
                # BE: create a comma separated string of the geom types observed
                geom_types = ",".join(geom_types)
                observed_geom_types = geom_types
                # BE: check that every geom is valid
                geom_valid_check =[]

                for obj in geoms:
                    try:
                        geom_valid_check.append(obj.is_valid)
                    except Exception as e:
                        geom_valid_check.append(False)
                    count += 1

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
                temp = gdf_as_df[gdf_as_df[col].notna()]  # Removes rows which are NA
                temp['checker'] = temp[col].str.match("\d+")  # TRUE = Only Digits, FALSE = Anything else

                if len(temp['checker']) == 0:
                    pct = 0
                else:
                    pct = temp['checker'].mean()

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
            logger.debug("NOTE: Dataframe does not contain any columns of object type")

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
        else:
            output_df['GeomTypesObserved'] = "N/A"
            output_df['InvalidGeometriesAtRows'] = "N/A"
        # Testing to make sure no stray columns have been inserted in the DQ Process ##
        if len(output_df) == len(list(gdf_as_df)):
            rows = output_df.values.tolist()
            gdf_dq_reports.append(rows)
        else:
            pass
            logger.debug(f"ERROR: The output dataframe does not have the "
                         f"same number of columns as the original input file. Cannot export.")

    return gdf_dq_reports
