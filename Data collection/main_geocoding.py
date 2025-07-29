'''
Main to perform geocoding
'''

import argparse
from nominatim_ner_geocoding import *


parser = argparse.ArgumentParser(description='Locations extraction and geocoding')
parser.add_argument('-operation_type',
                    type=str,
                    default='nominatim',
                    help='Operation type')
parser.add_argument('-data',
                    type=str,
                    help='Dataset path')
parser.add_argument('-loc_field',
                    type=str,
                    default='location',
                    help='Location field: it contains the location string to geocode')
parser.add_argument('-user_field',
                    type=str,
                    default='author_id',
                    help='User field')
parser.add_argument('-lang_field',
                    type=str,
                    default='lang',
                    help='Language field')
parser.add_argument('-italy_world',
                    type=str,
                    default=False,
                    help='False for world - True for Italy')
parser.add_argument('-org',
                    type=str,
                    default=False,
                    help='True for org - False for not org')
parser.add_argument('-threshold',
                    type=float,
                    default=0,
                    help='Threshold on Nominatim results')
parser.add_argument('-output',
                    type=str,
                    default='output.csv',
                    help='Output file')


args = parser.parse_args()

geocoder = NominatimLocGeocoder()

# only nominatim -> we use function that writes on csv step by step
if args.operation_type == 'nominatim':

    df = pd.read_csv(args.data)

    geocoder.find_tot_loc_nominatim_pipeline_csv(df, args.loc_field, args.user_field, args.output)