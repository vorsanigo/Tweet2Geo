import ast
import csv
import pandas as pd
import requests
import logging
from tqdm import tqdm
import time
from preprocessing import cleaning_ner_loc, cleaning_nominatim
from ner import NERLocation



class NominatimLocGeocoder():
    '''
    Nominatim geocoder based on OpenStreetMap
    Attributes:
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


    def find_loc_nominatim_italy(self, starting_location, cleaned_location, user, address_details, format, maxRows, lang):
        '''
        Given a location to find, cleaned location, twitter user id, Nominatim requests attributes,
        it returns a dictionary with the results for the searched location, useful for the main location
        It differs from 'find_loc_nominatim' on the field 'county', it is the function to use if we have Italian locations
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
        res_dict = {}


        if not pd.isna(starting_location):  # if nan starting location return {}

            if not pd.isna(cleaned_location): # if not nan results

                location = requests.get(
                    'https://nominatim.openstreetmap.org/?addressdetails=' + address_details + '&q=' +
                    cleaned_location + '&format=' + format + '&maxRows=' + str(maxRows) +
                    '&accept-language=' + lang)

                # accept-language (last field in the request) is set to en to avoid results in strange languages

                loc_json = location.json()

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
                            res_dict['granularity'] = 'city'
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

                else:  # if there are no results from nominatim

                    res_dict = {'user': user, 'location': starting_location,
                                'refactor location nominatim': cleaned_location, 'address': None, 'lat': None,
                                'lon': None, 'bbox': None, 'municipality': None,
                                'village': None, 'town': None, 'city': None, 'country': None, 'county': None,
                                'state': None,
                                'state_district': None, 'country_code': None, 'no_match': True, 'importance': None,
                                'granularity': None}

            else:  # if nan cleaned location

                res_dict = {'user': user, 'location': starting_location,
                            'refactor location nominatim': cleaned_location,
                            'address': None, 'lat': None, 'lon': None, 'bbox': None, 'municipality': None,
                            'village': None, 'town': None, 'city': None, 'country': None, 'county': None, 'state': None,
                            'state_district': None, 'country_code': None, 'no_match': True, 'importance': None,
                            'granularity': None}

        return res_dict


    def find_loc_nominatim_ner(self, starting_location, locations_ner, user):
        '''
        Given the starting location, the entities recognized as location by NER, the user, it applies nominatim search
        on the extracted location entities, useful for the main location
        :param starting_location: initial location
        :param locations_ner: locations entities
        :param user: twitter user id
        :return: a list of dictionaries with the searched locations,
        '''


        if locations_ner != []: # if there are location entities from NER

            loc_res = []

            for location in locations_ner: # send locations found with NER to nominatim
                res = self.find_loc_nominatim(starting_location, location, user, self.address_details, self.format,
                                              self.maxRows, self.lang)
                res['locations NER'] = locations_ner
                loc_res.append(res)
                time.sleep(self.sleep_time)

        else: # if there are no location entities from NER

            loc_res = [{'user': user, 'location': starting_location, 'refactor location nominatim': None,
                        'locations NER': locations_ner, 'address': None, 'lat': None, 'lon': None, 'bbox': None,
                        'municipality': None, 'village': None, 'town': None, 'city': None, 'country': None,
                        'county': None, 'state': None, 'state_district': None, 'country_code': None, 'no_match': True,
                        'importance': None, 'granularity': None}]

        return loc_res


    def find_loc_nominatim_ner_italy(self, starting_location, locations_ner, user):
        '''
        Given the starting location, the entities recognized as location by NER, the user, it applies nominatim search
        on the extracted location entities, useful for the main location
        Use this function if we have Italian locations
        :param starting_location: initial location
        :param locations_ner: locations entities
        :param user: twitter user id
        :return: a list of dictionaries with the searched locations
        '''


        if locations_ner != []: # if there are locations entities from NER

            loc_res = []

            for location in locations_ner:
                res = self.find_loc_nominatim_italy(starting_location, location, user, self.address_details,
                                                    self.format, self.maxRows, self.lang)
                res['locations NER'] = locations_ner
                loc_res.append(res)
                time.sleep(self.sleep_time)

        else: # if there are no location entities from NER

            loc_res = [{'user': user, 'location': starting_location, 'locations NER': locations_ner, 'address': None,
                        'lat': None, 'lon': None, 'bbox': None, 'municipality': None, 'village': None, 'town': None,
                        'city': None, 'country': None, 'county': None, 'state': None, 'state_district': None,
                        'country_code': None, 'no_match': True, 'importance': None, 'granularity': None}]

        return loc_res



    def reduce_results_ner(self, res_nominatim_ner):
        '''
        Given the results from NER -> Nominatim, it reduced them by combining places (eg: if there is a city and its
        country, we keep only the city since it already has the information about the country), useful for the main
        location
        :param res_nominatim_ner: results from NER -> Nominatim
        :return: list of dictionaries with the searched locations
        '''



        countries = []
        states = []
        cities = []

        user = ''
        location = ''
        refactor_location_nom = ''
        locations_ner = ''

        for el in res_nominatim_ner:

            user = el['user']
            location = el['location']
            refactor_location_nom = el['refactor location nominatim']
            locations_ner = el['locations NER']

            if el['locations NER'] == [] or el['address'] == None:  # no Named Entities found / no result found calling Nominatim
                res_nominatim_ner.remove(el)
            elif el['granularity'] == 'country':
                countries.append(el)
            elif el['granularity'] == 'state':
                states.append(el)
            elif el['granularity'] == 'city':
                cities.append(el)

        for city in cities:
            for state in states:
                options_state = {state['state'], state['state_district'], state['county']}
                if None in options_state:
                    options_state.remove(None)
                if len(options_state) != 0 and (city['state'] in options_state or city['state_district'] in
                                                options_state or city['county'] in options_state):
                    states.remove(state)
                    if state in res_nominatim_ner:
                        res_nominatim_ner.remove(state)
            for country in countries:
                if city['country'] == country['country']:
                    countries.remove(country)
                    if country in res_nominatim_ner:
                        res_nominatim_ner.remove(country)

        for state in states:
            for country in countries:
                if state['country'] == country['country']:
                    countries.remove(country)
                    if country in res_nominatim_ner:
                        res_nominatim_ner.remove(country)


        if res_nominatim_ner == []:

            res_nominatim_ner = [{'user': user, 'location': location, 'refactor location nominatim':
            refactor_location_nom, 'locations NER': locations_ner, 'address': None, 'lat': None, 'lon': None,
            'bbox': None, 'municipality': None, 'village': None, 'town': None, 'city': None, 'country': None,
            'county': None, 'state': None, 'state_district': None, 'country_code': None, 'no_match': True,
            'importance': None, 'granularity': None}]


        return res_nominatim_ner


    def reduce_results_ner_italy(self, res_nominatim_ner):
        '''
        Given the results from NER -> Nominatim, it reduced them by combining places (eg: if there is a city and its
        country, we keep only the city since it already has the information about the country), we consider in different
        ways the field 'country' with respect to 'reduce_results_ner', useful for the main location
        Use this function if we have Italian locations
        :param res_nominatim_ner: results from NER -> Nominatim
        :return: list of dictionarieswith the searched locations
        '''


        countries = []
        states = []
        cities = []

        user = ''
        location = ''
        refactor_location_nom = ''
        locations_ner = ''

        for el in res_nominatim_ner:
            user = el['user']
            location = el['location']
            refactor_location_nom = el['refactor location nominatim']
            locations_ner = el['locations NER']

            if el['locations NER'] == [] or el['address'] == None:  # no Named Entities found / no result found calling Nominatim
                res_nominatim_ner.remove(el)
            elif el['granularity'] == 'country':
                countries.append(el)
            elif el['granularity'] == 'state':
                states.append(el)
            elif el['granularity'] == 'city':
                cities.append(el)

        for city in cities:
            for state in states:
                options_state = {state['state'], state['state_district']}
                if None in options_state:
                    options_state.remove(None)
                if len(options_state) != 0 and (city['state'] in options_state or city['state_district'] in options_state):
                    states.remove(state)
                    if state in res_nominatim_ner:  # []
                        res_nominatim_ner.remove(state)
            for country in countries:
                if city['country'] == country['country']:
                    countries.remove(country)
                    if country in res_nominatim_ner:
                        res_nominatim_ner.remove(country)

        for state in states:
            for country in countries:
                if state['country'] == country['country']:
                    countries.remove(country)
                    if country in res_nominatim_ner:
                        res_nominatim_ner.remove(country)


        if res_nominatim_ner == []:
            res_nominatim_ner = [{'user': user, 'location': location, 'refactor location nominatim':
                                refactor_location_nom, 'locations NER': locations_ner}]


        return res_nominatim_ner



    def nominatim_ner_pipeline(self, starting_location, user, lang, flags, weird_words, threshold, lang_corenlp,
                               lang_stanza, org, italy):
        '''
        Complete geocoding system: Nominatim -> check threshold -> NER -> Nominatim -> check threshold, useful for the
        main location
        :param starting_location: initial location
        :param user: twitter user id
        :param lang: location language
        :param flags: list of flags info
        :param weird_words: list of weird words
        :param threshold: theshold to filter results
        :param lang_corenlp: dictionary with coreNLP langiages
        :param lang_stanza: list with stanza langiages
        :param org: True if we want to keep organizations entities as locations, False otherwise
        :param italy: True if we have Italian locations, False otherwise
        :return: list of dictionaries of the searches locations words for a user location (it can be multiple places)
        '''

        if not pd.isna(starting_location): # if not nan starting location clean it
            cleaned_location = cleaning_nominatim(starting_location, flags, weird_words)

        else: # if nan starting location keep it as it is
            cleaned_location = starting_location

        print('cleaned location', cleaned_location)

        if not italy: # if not Italian locations use normal function
            res_nominatim = self.find_loc_nominatim(starting_location, cleaned_location, user, self.address_details,
                                                    self.format, self.maxRows, self.lang)

        else: # if Italian locations use special function for Italy
            res_nominatim = self.find_loc_nominatim_italy(starting_location, cleaned_location, user,
                                                          self.address_details,
                                                          self.format, self.maxRows, self.lang)

        if res_nominatim != {} and list(res_nominatim.keys()) != ['user']: # if location field is not empty

            if res_nominatim['address'] == None or res_nominatim['importance'] < threshold:  # no results with nominatim -> use ner
                ner_locator = NERLocation()
                # todo keep this? start
                ner_locator.client.start()
                print('starting', starting_location)
                cleaned_ner_loc = cleaning_ner_loc(starting_location, flags)
                ner_entities, to_write_list = ner_locator.ner(cleaned_ner_loc, ner_locator.client, lang, user, lang_corenlp,
                                                              lang_stanza, org)
                if italy:
                    res_ner = self.find_loc_nominatim_ner_italy(starting_location, ner_entities, user)
                    ner_results_reduced = self.reduce_results_ner_italy(res_ner)
                    final_res = ner_results_reduced
                else:
                    res_ner = self.find_loc_nominatim_ner(starting_location, ner_entities, user)
                    ner_results_reduced = self.reduce_results_ner(res_ner)
                    final_res = ner_results_reduced
                # todo keep this? stop
                ner_locator.client.stop()

            else: # keep not empty nominatim result
                final_res = [res_nominatim]

        else: # keep empty nominatim resul
            final_res = [res_nominatim]

        print('final result', final_res)

        return final_res


    def find_tot_loc_nominatim_ner_pipeline(self, data, starting_location_field, user_field, lang_field, file_flags,
                                            file_words, threshold, lang_corenlp, lang_stanza, org, italy):
        '''
        It applies 'nominatim_ner_pipeline' on a dataset of twitter locations, it returns all the found locations in a
        list and it puts them into a dataframe, useful for the main location
        :param data: dataset of tweets
        :param starting_location_field: field of tweet location
        :param user_field: field of twitter user id
        :param lang_field: field of tweet language
        :param file_flags: file of flags
        :param file_words: file of weird words
        :param threshold: threshold to filter the results out considering the importance
        :param lang_corenlp: file of coreNLP languages
        :param lang_stanza: file of stanza languages
        :param org: True if we want to keep organizations entities as locations, False otherwise
        :param italy: True if we have Italian locations, False otherwise
        :return: list of dictionaries with locations, dataframe of found locations
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("Geocoder")

        tot_results = []

        flags_df = pd.read_csv(file_flags, sep='\t')
        flags = list(flags_df['country_code'])
        weird_words = pd.read_csv(file_words, sep='\t')

        output_df = pd.DataFrame(columns=[])

        for i in range(len(data)):
            starting_loc = data.at[i, starting_location_field]
            user = data.at[i, user_field]
            lang = data.at[i, lang_field]
            loc_res = self.nominatim_ner_pipeline(starting_loc, user, lang, flags, weird_words, threshold, lang_corenlp,
                                                  lang_stanza, org, italy)

            for el in loc_res:
                if el != {}:
                    tot_results.append(el)
                    output_df = output_df.append(el, ignore_index=True)
            pbar.update(1)
        pbar.close()

        return tot_results, output_df


    def find_tot_loc_nominatim_ner_pipeline_tsv(self, data, starting_location_field, user_field, lang_field, file_flags,
                                            file_words, threshold, lang_corenlp, lang_stanza, org, italy, output_file):
        '''
        It applies 'nominatim_ner_pipeline' on a dataset of twitter locations, it returns all the found locations in a
        list and it writes them into a csv file step by step instead, useful for the main location
        :param data: dataset of tweets
        :param starting_location_field: field of tweet location
        :param user_field: field of twitter user id
        :param lang_field: field of tweet language
        :param file_flags: file of flags
        :param file_words: file of weird words
        :param threshold: threshold to filter the results out considering the importance
        :param lang_corenlp: file of coreNLP languages
        :param lang_stanza: file of stanza languages
        :param org: True if we want to keep organizations entities as locations, False otherwise
        :param italy: True if we have Italian locations, False otherwise
        :param output_file: csv output file
        :return: list of dictionaries with locations, dataframe of found locations
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("Geocoder")

        flags_df = pd.read_csv(file_flags, sep='\t')
        flags = list(flags_df['country_code'])
        weird_words = pd.read_csv(file_words, sep='\t')

        csv_columns = ['user', 'location', 'refactor location nominatim', 'locations NER', 'address', 'lat', 'lon',
                       'bbox', 'municipality', 'village', 'town', 'city', 'country', 'county', 'state', 'state_district',
                       'country_code', 'no_match', 'importance', 'granularity']

        csv_file = output_file

        with open(csv_file, 'a') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()

            for i in range(len(data)):
                starting_loc = data.at[i, starting_location_field]
                user = data.at[i, user_field]
                lang = data.at[i, lang_field]
                loc_res = self.nominatim_ner_pipeline(starting_loc, user, lang, flags, weird_words, threshold,
                                                      lang_corenlp, lang_stanza, org, italy)

                try:
                    for loc in loc_res:
                        print('location to write', loc)
                        writer.writerow(loc)
                except IOError:
                    print("I/O error")
                pbar.update(1)
            pbar.close()



    def nominatim_pipeline(self, starting_location, user, flags, weird_words, italy):
        '''
        Nominatim only pipeline
        :param starting_location: initial location
        :param user: twitter user id
        :param flags: list of flags info
        :param weird_words: list of weird words
        :param italy: True if we have Italian locations, False otherwise
        :return: list of dictionaries of the searches locations words for a user location (it can be multiple places)
        '''

        if not pd.isna(starting_location): # if not nan starting location clean it
            cleaned_location = cleaning_nominatim(starting_location, flags, weird_words)

        else: # if nan starting location keep it as it is
            cleaned_location = starting_location

        print('cleaned location', cleaned_location)

        if not italy: # if not Italian locations use normal function
            res_nominatim = self.find_loc_nominatim(starting_location, cleaned_location, user, self.address_details,
                                                    self.format, self.maxRows, self.lang)

        else: # if Italian locations use special function for Italy
            res_nominatim = self.find_loc_nominatim_italy(starting_location, cleaned_location, user,
                                                          self.address_details,
                                                          self.format, self.maxRows, self.lang)

        print('final result', res_nominatim)

        return res_nominatim


    def find_tot_loc_nominatim_pipeline(self, data, starting_location_field, user_field, file_flags,
                                            file_words, italy):
        '''
        It applies 'nominatim_pipeline' on a dataset of twitter locations, it returns all the found locations in a
        list and it puts them into a dataframe, useful for the main location
        :param data: dataset of tweets
        :param starting_location_field: field of tweet location
        :param user_field: field of twitter user id
        :param file_flags: file of flags
        :param file_words: file of weird words
        :param italy: True if we have Italian locations, False otherwise
        :return: list of dictionaries with locations, dataframe of found locations
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("Geocoder")

        tot_results = []

        flags_df = pd.read_csv(file_flags, sep='\t')
        flags = list(flags_df['country_code'])
        weird_words = pd.read_csv(file_words, sep='\t')

        output_df = pd.DataFrame(columns=[])

        for i in range(len(data)):
            starting_loc = data.at[i, starting_location_field]
            user = data.at[i, user_field]
            loc_res = self.nominatim_pipeline(starting_loc, user, flags, weird_words, italy)

            tot_results.append(loc_res)
            output_df = output_df.append(loc_res, ignore_index=True)
            pbar.update(1)
        pbar.close()


        return tot_results, output_df


    def find_tot_loc_nominatim_pipeline_tsv(self, data, starting_location_field, user_field, file_flags,
                                        file_words, italy, output_file):
        '''
        It applies 'nominatim_pipeline' on a dataset of twitter locations, it returns all the found locations in a
        list and it writes them into a csv file step by step, useful for the main location
        :param data: dataset of tweets
        :param starting_location_field: field of tweet location
        :param user_field: field of twitter user id
        :param file_flags: file of flags
        :param file_words: file of weird words
        :param italy: True if we have Italian locations, False otherwise
        :param output_file: csv output file
        :return: list of dictionaries with locations, dataframe of found locations
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("Geocoder")

        flags_df = pd.read_csv(file_flags, sep='\t')
        flags = list(flags_df['country_code'])
        weird_words = pd.read_csv(file_words, sep='\t')

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
                loc_res = self.nominatim_pipeline(starting_loc, user, flags, weird_words, italy)

                print('uff', loc_res)

                try:
                    print('location to write', loc_res)
                    writer.writerow(loc_res)
                except IOError:
                    print("I/O error")


    def find_loc_text_nominatim(self, list_starting_location, tweet_id, address_details, format,
                                maxRows, lang, flags, weird_words):
        '''
        Given a list of starting locations extracted from the tweet text, the corresponding cleaned ones to pass to
        nominatim, the tweet id, the nominatim search parameters, it finds the matches for the locations using nominatim,
        useful for text locations
        :param list_starting_location: locations extracted from the text
        :param list_cleaned_location: cleaned locations extracted from the text
        :param tweet_id: tweet id
        :param address_details:
        :param format:
        :param maxRows:
        :param lang:
        :return: list of dictionaries containing the found locations using nominatim or NAN if there are no results for
        the text locations
        '''

        res = []

        if not pd.isna(list_starting_location): # if not nan NER locations send it to nominatim

            list_starting_location = ast.literal_eval(list_starting_location)
            list_cleaned_location = []

            for loc in list_starting_location:
                cleaned_loc = cleaning_nominatim(loc, flags, weird_words)
                list_cleaned_location.append(cleaned_loc)

            if len(list_cleaned_location) != 0:

                for cleaned_location in list_cleaned_location:

                    res_dict = {}

                    location = requests.get('https://nominatim.openstreetmap.org/?addressdetails=' + address_details + '&q='
                                            + cleaned_location + '&format=' + format + '&maxRows=' + str(maxRows) +
                                            '&accept-language=' + lang)

                    # accept-language (last field in the request) is set to en to avoid results in strange languages

                    loc_json = location.json()

                    if loc_json != []: # if result from nominatim

                        num = 0

                        while num < maxRows and num < len(loc_json):

                            loc = loc_json[num]

                            res_dict['tweet_id'] = tweet_id
                            res_dict['location'] = list_starting_location
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

                        res.append(res_dict)

                    else:  # if there are no results from nominatim

                        res.append({'tweet_id': tweet_id, 'location': list_starting_location, 'refactor location nominatim':
                                    cleaned_location, 'address': None, 'lat': None, 'lon': None, 'bbox': None,
                                    'municipality': None, 'village': None, 'town': None, 'city': None, 'country': None,
                                    'county': None, 'state': None, 'state_district': None, 'country_code': None,
                                    'no_match': True, 'importance': None, 'granularity': None})


        return res


    def find_tot_loc_text_nominatim(self, data, list_starting_loc_field, tweet_id_field, file_flags, file_words):
        '''
        Given a dataset of tweets, it maps the extracted locations from the tweets text to the corresponding locations
        in nominatim, it saves the results in a dataframe useful for text location
        :param data: dataset
        :param list_starting_loc_field: field of starting locations
        :param tweet_id_field: field of tweets ids
        :param file_flags: file of flags
        :param file_words: file of weird words
        :return: list of total dictionaries locations, dataframe of found locations
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("Geocoder")

        tot_results = []

        flags_df = pd.read_csv(file_flags, sep='\t')
        flags = list(flags_df['country_code'])
        weird_words = pd.read_csv(file_words, sep='\t')

        output_df = pd.DataFrame(columns=[])

        for i in range(len(data)):
            list_starting_loc = data.at[i, list_starting_loc_field]
            tweet_id = data.at[i, tweet_id_field]

            loc_res = self.find_loc_text_nominatim(list_starting_loc, tweet_id, self.address_details,
                                                   self.format, self.maxRows, self.lang, flags, weird_words)

            if len(loc_res) > 0:
                for el in loc_res:
                    tot_results.append(el)
                    output_df = output_df.append(el, ignore_index=True)

            pbar.update(1)
        pbar.close()

        return tot_results, output_df





'''df_unicodes = pd.read_csv('unicode_flags.csv', sep='\t') # TODO put ue flag in the list
list_flags = list(df_unicodes['country_code'])
df_weird_words = pd.read_csv('weird words 1.csv')
weird_words = list(df_weird_words['word'])'''
#cleaned = cleaning_nominatim('🇨🇱 Barcelona, España 🇪🇦 ', list_flags, weird_words)
#print(cleaned)

'''n = NominatimLocGeocoder()

df = pd.read_csv('Tweets Italy 1-7 -- 13-7/locations_coreNLP.csv', sep='\t')
x,y = n.find_tot_loc_text_nominatim(df, 'found_locations', 'id', 'unicode_flags.csv', 'weird words 1.csv')
y.to_csv('Tweets Italy 1-7 -- 13-7/nom_loc_coreNLP.csv')'''

'''tweets = pd.read_csv('Tweets Italy 1-7 -- 13-7/tweets_df.csv')
loc = pd.read_csv('prova0.csv')
loc = loc.rename(columns={'tweet_id': 'id'})
df0 = pd.merge(tweets, loc, on='id', how='inner')
df0.to_csv('prova0_loc.csv')
loc0 = pd.read_csv('prova00.csv')
loc0 = loc0.rename(columns={'tweet_id': 'id'})
df00 = pd.merge(tweets, loc0, on='id', how='inner')
df00.to_csv('prova00_loc.csv')'''


'''print(n.address_details)
print(n.format)
print(n.lang)
print(n.maxRows)
x = n.find_loc_nominatim('🇨🇱 Barcelona, España 🇪🇦 ', cleaned, 111, n.address_details, n.format, n.maxRows, n.lang)
print(x)'''

'''res = n.find_loc_nominatim_ner('🇨🇱 Barcelona, España 🇪🇦 ', ['Chile', 'Barcelona', 'España'], 111)
res1 = n.reduce_results_ner(res)
print(res1)'''


dict_corenlp = {'it': {'cdc_tokenize.model': 'edu/stanford/nlp/models/cdc-tokenize/it-tokenizer.ser.gz',
                       'cdc_tokenize.multiWordRules': 'edu/stanford/nlp/models/cdc-tokenize/it-multiword.txt',
                       #'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                       'ner.model': 'edu/stanford/nlp/models/ner/italian.crf.ser.gz',
                       'ner.applyFineGrained' : 'false',
                       'ner.applyNumericClassifiers' : 'false',
                       'ner.useSUTime' : 'false'
                       },
                'en': {#'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                       'ner.language' : 'en',
                       'ner.applyFineGrained' : 'false'},
                'fr': {'tokenize.language': 'fr',
                       #'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                      'ner.model': 'edu/stanford/nlp/models/ner/french-wikiner-4class.crf.ser.gz',
                       'ner.applyFineGrained' : 'false',
                       'ner.applyNumericClassifiers' : 'false',
                       'ner.useSUTime' : 'false'
                       },
                'de': {'tokenize.language': 'de',
                       'tokenize.postProcessor' : 'edu.stanford.nlp.international.german.process.GermanTokenizerPostProcessor',
                       #'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                       'ner.model': 'edu/stanford/nlp/models/ner/german.distsim.crf.ser.gz',
                        'ner.applyFineGrained' : 'false',
                       'ner.applyNumericClassifiers' : 'false',
                       'ner.useSUTime' : 'false'
                       },
                'hu': {'cdc_tokenize.model': 'edu/stanford/nlp/models/cdc-tokenize/hu-tokenizer.ser.gz',
                       #'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                       'ner.model': 'edu/stanford/nlp/models/ner/hungarian.crf.ser.gz',
                       'ner.applyFineGrained' : 'false',
                       'ner.applyNumericClassifiers' : 'false',
                       'ner.useSUTime' : 'false'
                       },
                'es': {'tokenize.language': 'es',
                       #'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                       'ner.model': 'edu/stanford/nlp/models/ner/spanish.ancora.distsim.s512.crf.ser.gz',
                        'ner.applyFineGrained' : 'false',
                       'ner.applyNumericClassifiers' : 'false',
                       'ner.useSUTime' : 'false',
                       'ner.language' : 'es'
                       },
                'zh': {'tokenize.language': 'zh',
                       #'tokenize.whitespace': 'true',
                       #'ssplit.eolonly' : 'true',
                       'segment.model' : 'edu/stanford/nlp/models/segmenter/chinese/ctb.gz',
                        'segment.sighanCorporaDict' : 'edu/stanford/nlp/models/segmenter/chinese',
                        'segment.serDictionary' : 'edu/stanford/nlp/models/segmenter/chinese/dict-chris6.ser.gz',
                        'segment.sighanPostProcessing' : 'true',
                       'ner.language': 'chinese',
                       'ner.model': 'edu/stanford/nlp/models/ner/chinese.misc.distsim.crf.ser.gz',
                        'ner.applyFineGrained' : 'false',
                       'ner.applyNumericClassifiers' : 'false',
                        'ner.useSUTime' : 'false'
                       },
                'not listed': {#'tokenize.whitespace': 'true',
                                #'ssplit.eolonly' : 'true',
                               'ner.applyFineGrained' : 'false'},
                }

list_lang_stanza = ['af', 'ar', 'bg', 'zh', 'nl', 'en', 'fi', 'fr', 'de', 'hu', 'it', 'my', 'ru', 'es', 'uk', 'vi']

'''
with open('lang_coreNLP.json', 'w') as outfile:
    json.dump(dict_corenlp, outfile)

with open('lang_stanza.json', 'w') as outfile:
    json.dump(list_lang_stanza, outfile)'''



'''res = n.nominatim_ner_pipeline('🇨🇱 Barcelona, España 🇪🇦 ', 111, n.lang, list_flags, weird_words, 0.6, dict_corenlp, list_lang_stanza, False, False)
print(res)'''

'''d = pd.read_csv('Marmolada/results to use/tweets datasets/tweets_marmolada 1-14.csv')
d = d.drop_duplicates(subset=['location']).reset_index(drop=True)
#d = d.dropna(subset=['location']).reset_index(drop=True)
res, df = n.find_tot_loc_nominatim_ner_pipeline(d, 'location', 'author_id', 'lang', 'unicode_flags.csv', 'weird words 1.csv', 0.6, dict_corenlp, list_lang_stanza, False, False)
print(df)
df.to_csv('sss.csv')'''


'''df = pd.read_csv('sss.csv')
d = pd.read_csv('Marmolada/results to use/tweets datasets/tweets_marmolada 1-14.csv')
df3 = pd.merge(d, df, on=['location'], how='inner')

df3.to_csv('mere.csv')'''

'''df_locations = pd.read_csv('Marmolada/results to use/tweets datasets/com_loc_tweets_marmolada 1-14.csv')
df_iso = pd.read_csv('Marmolada/country_iso/iso_codes_com.csv')
list_iso_2 = list(df_iso['iso_a2'])
for i in range(len(list_iso_2)):
    if pd.isna(list_iso_2[i]):
        list_iso_2[i] = 'NA'
list_iso_2 = [str(x).lower() for x in list_iso_2]
list_iso_3 = list(df_iso['iso_a3'])
for i in range(len(df_locations)):
    if df_locations.at[i, 'country_code'] in list_iso_2:
        df_locations.at[i, 'iso_3'] = list_iso_3[list_iso_2.index(df_locations.at[i, 'country_code'])]
df_locations.to_csv('Marmolada/results to use/tweets datasets/com_loc_tweets_marmolada_iso3.csv')'''


#x = NominatimLocGeocoder()
'''dir = 'Nations tweets/4 tweets/'
files = os.listdir(dir)

for file in files:
    df = pd.read_csv(dir+file)
    res, df = x.find_tot_loc_nominatim_pipeline(df, 'location', 'id', 'unicode_flags.csv', 'weird words 1.csv', italy=False)
    df.to_csv(dir+file+'_loc_check.csv')'''

#df = pd.read_csv('650_world_cleaned.csv', sep='\t').head(10)
#x.find_tot_loc_nominatim_ner_pipeline_tsv(df, 'location', 'user', 'lang', 'unicode_flags.csv', 'weird words 1.csv', 0.6, dict_corenlp, list_lang_stanza, False, False, 'output_ner.csv')

# tweet0, 1, 2, 3, 4 -> in screen 1, 2, 3, 4, 5





