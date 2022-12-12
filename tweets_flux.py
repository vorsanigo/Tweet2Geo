'''
Functions to extract locations and map them through a geocoding system (Nominatim)
'''

# 1) twitter api -> download tweets
# 2) tweets dataset -> put tweets in a dataset
# 3) tweets flux -> extract locations and map locations to geocoding system

from nominatim_ner_geocoding import *

list_c = ['germany', 'great britain', 'hungary', 'ireland', 'liechtenstein', 'lituania', 'luxembourg', 'malta', 'monaco', 'netherlands', 'poland', 'slovenia', 'spain', 'switzerland', 'ukraine']


def extract_locations(country_file, id_col, loc_col, tweets_num, output_file):
    '''
    Extract all the locations mentioned in the tweets of a single country
    :param country_file:
    :param id_col: tweet ids column
    :param loc_col: locations column
    :param tweets_num: number of tweets to consider
    :param output_file: output file with ids - locations
    :return: dataframe with ids - locations
    '''

    df = pd.read_csv(country_file).head(tweets_num)
    print(df)
    new_df = pd.DataFrame(columns=[id_col, loc_col])

    for i in range(len(df)):
        if not pd.isna(df.at[i, loc_col]):
            list_loc = ast.literal_eval(df.at[i, loc_col])
            print(list_loc)
            for loc in list_loc:
                new_df = new_df.append({id_col: df.at[i, id_col], loc_col: loc}, ignore_index=True)
        else:
            new_df = new_df.append({id_col: df.at[i, id_col], loc_col: pd.NA}, ignore_index=True)

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

    df = pd.read_csv(locations_file)
    nom = NominatimLocGeocoder()
    new = pd.DataFrame(columns=[])

    for i in range(len(df)):
        print('numero', i)
        x = nom.find_loc_nominatim(df.at[i, loc_col], df.at[i, loc_col], df.at[i, id_col], nom.address_details, nom.format, nom.maxRows, nom.lang)
        new = new.append(x, ignore_index=True)

    new.to_csv(output_file)


def main(locations_file, output_loc_file1, output_loc_file2):
    extract_locations(locations_file, 'id', 'loc_text', 30000, output_loc_file1)
    find_locations(output_loc_file1, 'id', 'loc_text', output_loc_file2)

if __name__ == '__main__':
    main('Nations tweets/AT1.csv', 'Nations tweets/0 locations/AT_loc1.csv', 'Nations tweets/0 locations/AT_found_loc1.csv')