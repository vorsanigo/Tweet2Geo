'''
Class to perform geocoding
'''

import ast
import csv
import pandas as pd
import requests
import logging
from tqdm import tqdm
import time
from ner import NERLocation



class NominatimLocGeocoder():
    '''
    Nominatim geocoder based on OpenStreetMap
    Attributes:
        - server_url
        - address_details
        - format: results format
        - maxRows: max number of results
        - lang: results language
        - sleep_time: sleep time for Nominatim requests
    '''

    def __init__(self, server_url = 'https://nominatim.openstreetmap.org/', address_details = '?addressdetails=1&q=',
                 format = 'json', maxRows = 1, lang = 'en', sleep_time = 0.2):

        self.server_url = server_url
        self.address_details = address_details
        self.format = format
        self.maxRows = maxRows
        self.lang = lang
        self.sleep_time = sleep_time


    def find_loc_nominatim(self, starting_location, cleaned_location, user, address_details, format, maxRows, lang):
        '''
        Given a location to find, cleaned location, twitter user id, Nominatim requests attributes,
        it returns a dictionary with the results for the searched location, useful for the main location
        :param starting_location: twitter location
        :param cleaned_location: cleaned location
        :param user: user id
        :param address_details: details for the search
        :param format: format of the result
        :param maxRows: max number of results to retrieve (set to 1)
        :param lang: results language (set to en)
        :return: dictionary of the geocoded location, keys: user, location
        '''

        num = 0
        res_dict = {'user': user}


        if not pd.isna(starting_location): # if nan starting location return {}

            if not pd.isna(cleaned_location): # if not nan cleaned location

                # query to nominatim
                location = requests.get('https://nominatim.openstreetmap.org/?addressdetails=' + address_details + '&q='
                                        + cleaned_location + '&format=' + format + '&maxRows=' + str(maxRows) +
                                        '&accept-language=' + lang)

                # accept-language (last field in the request) is set to en to avoid results in strange languages

                try:
                    loc_json = location.json()
                    print('loccccccccccc', loc_json)

                    if loc_json != []:

                        while num < maxRows and num < len(loc_json):

                            loc = loc_json[num]

                            res_dict['user'] = user
                            res_dict['location'] = starting_location
                            res_dict['refactor location nominatim'] = cleaned_location
                            res_dict['granularity'] = ''

                            # we keep all the results, we filter them afterwards comparing importance with threshold

                            if 'importance' in loc.keys():
                                res_dict['importance'] = loc['importance']
                            else:
                                res_dict['importance'] = None
                            if 'address' in loc.keys():
                                address = loc['address']
                                res_dict['address'] = loc['address']
                            else:
                                address = None
                                res_dict['address'] = None
                            if 'lon' in loc.keys():
                                res_dict['lon'] = loc['lon']
                            else:
                                res_dict['lon'] = None
                            if 'lat' in loc.keys():
                                res_dict['lat'] = loc['lat']
                            else:
                                res_dict['lat'] = None
                            if 'boundingbox' in loc.keys():  # lat, lat, lon, lon
                                 res_dict['bbox'] = loc['boundingbox']
                            else:
                                res_dict['bbox'] = None
                            if 'country' in address.keys():
                                res_dict['country'] = address['country']
                                res_dict['granularity'] = 'country'
                            else:
                                res_dict['country'] = None
                            if 'country_code' in address.keys():
                                res_dict['country_code'] = address['country_code']
                            else:
                                res_dict['country_code'] = None
                            if 'state' in address.keys():
                                res_dict['state'] = address['state']
                                res_dict['granularity'] = 'state'
                            else:
                                res_dict['state'] = None
                            if 'state_district' in address.keys():
                                res_dict['state_district'] = address['state_district']
                                res_dict['granularity'] = 'state'
                            else:
                                res_dict['state_district'] = None
                            if 'county' in address.keys():
                                res_dict['county'] = address['county']
                                res_dict['granularity'] = 'state'
                            else:
                                res_dict['county'] = None
                            if 'city' in address.keys():
                                res_dict['city'] = address['city']
                                res_dict['granularity'] = 'city'
                            else:
                                res_dict['city'] = None
                            if 'town' in address.keys():
                                res_dict['town'] = address['town']
                                res_dict['granularity'] = 'city'
                            else:
                                res_dict['town'] = None
                            if 'village' in address.keys():
                                res_dict['village'] = address['village']
                                res_dict['granularity'] = 'city'
                            else:
                                res_dict['village'] = None
                            if 'municipality' in address.keys():
                                res_dict['municipality'] = address['municipality']
                                res_dict['granularity'] = 'city'
                            else:
                                res_dict['municipality'] = None

                            num += 1

                    else: # if there are no results from nominatim

                        res_dict = {'user': user, 'location': starting_location, 'refactor location nominatim':
                                    cleaned_location, 'address': None, 'lat': None, 'lon': None, 'bbox': None,
                                    'municipality': None, 'village': None, 'town': None, 'city': None, 'country': None,
                                    'county': None, 'state': None, 'state_district': None, 'country_code': None,
                                    'no_match': True, 'importance': None, 'granularity': None}

                except:

                    res_dict = {'user': user, 'location': starting_location,
                                'refactor location nominatim': cleaned_location,
                                'address': None, 'lat': None, 'lon': None, 'bbox': None, 'municipality': None,
                                'village': None, 'town': None, 'city': None, 'country': None, 'county': None,
                                'state': None,
                                'state_district': None, 'country_code': None, 'no_match': True, 'importance': None,
                                'granularity': None}

            else: # if nan cleaned location

                res_dict = {'user': user, 'location': starting_location, 'refactor location nominatim': cleaned_location,
                            'address': None, 'lat': None, 'lon': None, 'bbox': None, 'municipality': None,
                            'village': None, 'town': None, 'city': None, 'country': None, 'county': None, 'state': None,
                            'state_district': None, 'country_code': None, 'no_match': True, 'importance': None,
                            'granularity': None}

        return res_dict
    


    def nominatim_pipeline_light(self, location, user):
        '''
        Nominatim only pipeline
        :param location: location
        :param user: twitter user id
        :return: list of dictionaries of the searches locations words for a user location (it can be multiple places)
        '''

        res_nominatim = self.find_loc_nominatim(location, location, user, self.address_details,
                                                    self.format, self.maxRows, self.lang)

        return res_nominatim



    def find_tot_loc_nominatim_pipeline_csv_light(self, data, starting_location_field, user_field, output_file):
        '''
        It applies 'nominatim_pipeline_light' on a dataset of twitter locations, it returns all the found locations in a
        list and it writes them into a csv file step by step
        :param data: dataset of tweets
        :param starting_location_field: field of tweet location
        :param user_field: field of twitter user id
        :param output_file: csv output file
        :return: list of dictionaries with locations, dataframe of found locations
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("Geocoder")

        csv_columns = ['user', 'location', 'refactor location nominatim', 'address', 'lat', 'lon', 'bbox',
                       'municipality', 'village', 'town', 'city', 'country', 'county', 'state', 'state_district',
                       'country_code', 'no_match', 'importance', 'granularity']

        csv_file = output_file

        with open(csv_file, 'a') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()

            for i in range(len(data)):

                starting_loc = data.at[i, starting_location_field]

                user = data.at[i, user_field]
                loc_res = self.nominatim_pipeline_light(starting_loc, user)

                try:
                    writer.writerow(loc_res)
                except IOError:
                    print("I/O error")