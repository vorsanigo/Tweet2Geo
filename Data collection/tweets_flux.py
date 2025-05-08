'''
Functions to extract locations and map them through a geocoding system (Nominatim)
'''

# 1) twitter api -> download tweets
# 2) tweets dataset -> put tweets in a dataset
# 3) tweets flux -> extract locations and map locations to geocoding system

from nominatim_ner_geocoding import *


def extract_locations(country_file, id_col, loc_col, output_file):
    '''
    Extract all the locations mentioned in the tweets of a single country
    :param country_file: file of tweets of the country
    :param id_col: tweet ids column
    :param loc_col: locations column
    :param output_file: output file with ids - locations
    :return: dataframe with ids - locations
    '''

    # read tweets dataset
    df = pd.read_csv(country_file)
    new_df = pd.DataFrame(columns=[id_col, loc_col])

    # remove rows that have no location inside
    df = df[~df[loc_col].isna()].reset_index(drop=True)
    
    # iterate through the rows of the dataframe to access locations field and extract them from their list
    for i in range(len(df)):
        list_loc = ast.literal_eval(df.at[i, loc_col])
        if len(list_loc) == 0:
            new_df = new_df.append({id_col: df.at[i, id_col], loc_col: pd.NA}, ignore_index=True)
        else:
            for loc in list_loc:
                new_df = new_df.append({id_col: df.at[i, id_col], loc_col: loc}, ignore_index=True)
        
    new_df.to_csv(output_file)


def extract_locations_wa(country_file, id_col, loc_col, output_file):
    '''
    Extract all the locations mentioned in the tweets of a single country
    :param country_file: file of tweets of the country
    :param id_col: tweet ids column
    :param loc_col: locations column
    :param output_file: output file with ids - locations
    :return: dataframe with ids - locations
    '''

    # read tweets dataset
    df = pd.read_csv(country_file)
    new_df = pd.DataFrame(columns=[id_col, loc_col])

    # remove rows that have no location inside
    df = df[~df[loc_col].isna()].reset_index(drop=True)
    
    # iterate through the rows of the dataframe to access locations field and extract them from their list
    for i in range(len(df)):
        list_loc = ast.literal_eval(df.at[i, loc_col])
        if len(list_loc) == 0:
            new_df = new_df._append({id_col: df.at[i, id_col], loc_col: pd.NA}, ignore_index=True)
        else:
            new_list_loc = []
            for loc in list_loc:
                new_list_loc += [loc.lower()]
            # if "washington" is in the list of locations, do not keep it separate from the others, but put it together with the others to identify that it is washington state, so to help the geocoder to identify the right location
            if 'washington' in new_list_loc:
                loc_tot = " ".join(str(x) for x in new_list_loc)
                new_df = new_df._append({id_col: df.at[i, id_col], loc_col: loc_tot}, ignore_index=True)
            else:
                for loc in list_loc:
                    new_df = new_df._append({id_col: df.at[i, id_col], loc_col: loc}, ignore_index=True)
        
    new_df.to_csv(output_file)


def find_locations(locations_file, id_col, loc_col, output_file):
    '''
    Map the found locations using Nominatim
    :param locations_file: locations file
    :param id_col: ids columns
    :param loc_col: locations column
    :param output_file: output file with mapped locations
    :return: dataframe with mapped locations
    '''

    # read locations file
    df_0 = pd.read_csv(locations_file)

    df = df_0.drop_duplicates(subset=loc_col).reset_index(drop=True) # loc_col should be 'loc_text'
    nom = NominatimLocGeocoder()
    new = pd.DataFrame(columns=[])

    for i in range(len(df)):
        x = nom.find_loc_nominatim(df.at[i, loc_col], df.at[i, loc_col], df.at[i, id_col], nom.address_details, nom.format, nom.maxRows, nom.lang)
        new = new.append(x, ignore_index=True)

    new.to_csv(output_file)


def pipeline(locations_file, id_column, loc_column, output_loc_file1, output_loc_file2):
    '''
    Extract locations from the tweets dataset and map them using Nominatim
    :param locations_file: file of tweets of the country
    :param id_column: tweet ids column -> 'id'
    :param loc_column: locations column -> 'loc_text'
    :param output_loc_file1: output file of extract_locations
    :param output_loc_file2: output file of find_locations
    '''

    extract_locations(locations_file, id_column, loc_column, output_loc_file1)
    find_locations(output_loc_file1, id_column, loc_column, output_loc_file2)