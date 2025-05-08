'''
Class to randomly create Twitter queries over a certain time period
'''

# how to make queries -> https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
# twarc -> https://github.com/DocNow/twarc | https://github.com/DocNow/twarc/tree/main/twarc

# 4 years -> divide into ranges of 2 minutes
# keep taking time ranges of 2 minutes until we have 30000 tweets
import pandas as pd
from twarc import Twarc2, expansions
import datetime
import time
import random
import json

import config


BEARER_TOKEN = config.BEARER_TOKEN


class Twitter_query():


    def create_slots(self, start_date, end_date, minutes):
        '''
        Create slots of time of n minutes between a start and end date
        :param start_date: datetime.datetime()
        :param end_date: datetime.datetime()
        :param minutes: int, number of minutes per slot
        :return: list of times within the selected dates
        '''

        # create slots of n minutes
        mintime = int(time.mktime(start_date.timetuple()))
        maxtime = int(time.mktime(end_date.timetuple()))
        nb_slots = (maxtime - mintime)//(minutes*60)
        # slots list
        list_random = []
        for RECORD in range(nb_slots):
            random_slot = random.randint(0, nb_slots)
            random_ts = mintime + minutes * 60 * random_slot
            random_datetime = datetime.datetime.fromtimestamp(random_ts)
            list_random.append(random_datetime)

        return list_random



    def collect_tweets_random_slots(self, query, output_file, info_file, list_random, max_results, slots_minutes):
        '''
        Collect n tweets in a time range within different random slots
        :param list_random: random time slots
        :param output_file: output file
        :param query: query
        :param max_results: number of tweets to retrieve
        :param slots_minutes: number of minutes per time slot
        :return: output file with collected tweets
        '''

        # count total number of tweets
        count_tweets = 0
        # count slots
        list_random_count = 0
        # count year
        year = 0

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        start = time.time()

        # retrieve desired quantity of tweets
        with open(output_file, 'a+') as file:

            while count_tweets < max_results:

                if list_random_count < len(list_random[year]):
                    #print(list_random[year][list_random_count])
                    for page in tw.search_all(query=query,
                                           start_time=list_random[year][list_random_count]-datetime.timedelta(minutes=slots_minutes),
                                           end_time=list_random[year][list_random_count]):
                        tweets = expansions.flatten(page)
                        count_tweets_page = 0
                        while count_tweets_page < len(tweets) and count_tweets < max_results:
                            file.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                            count_tweets += 1
                            count_tweets_page += 1
                else:
                    break
                year += 1
                if year == 4:
                    list_random_count += 1
                    year = 0

        end = time.time()

        execution_time = end - start

        with open(info_file, 'a+') as file:
            file.write('Number of slots of ' + str(slots_minutes) + ': ' + str(list_random_count) + '\nTime to download tweets: ' + str(execution_time) + ' seconds')


    def collect_tweets_random_slots_countries(self, list_countries, output_file, list_random, max_results, slots_minutes):
        '''
        Collect n tweets in a time range within different random slots (picked within a week or another time period)
        and for different countries
        :param list_countries: list of countries from which we retrieve tweets
        :param output_file: output file's name
        :param list_random: random time slots
        :param max_results: number of tweetsto retrieve
        :param slots_minutes: number of minutes per time slot
        :return: output file with collected tweets
        '''

        # count total number of tweets
        count_tweets = 0
        # count slots
        list_random_count = 0
        # count country list
        list_countries_count = 0

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        start = time.time()

        # retrieve desired quantity of tweets
        with open(output_file, 'a+') as file:

            while count_tweets < max_results:

                query = 'place_country:' + list_countries[list_countries_count]

                if list_random_count < len(list_random):

                    for page in tw.search_all(query=query,
                                              start_time=list_random[list_random_count] - datetime.timedelta(
                                                  minutes=slots_minutes),
                                              end_time=list_random[list_random_count]):
                        tweets = expansions.flatten(page)
                        count_tweets_page = 0
                        while count_tweets_page < len(tweets) and count_tweets < max_results:
                            print('Number of tweets retrieved: ', str(count_tweets))
                            file.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                            count_tweets += 1
                            count_tweets_page += 1
                else:
                    break

                list_countries_count +=1
                # if we went across all the countries, go to next slot
                if list_countries_count == len(list_countries):
                    list_countries_count = 0
                    list_random_count += 1

        end = time.time()

        execution_time = end - start

        print('Execution time: ' + str(execution_time))

        '''with open(info_file, 'a+') as file:
            file.write('Number of slots of ' + str(slots_minutes) + ': ' + str(
                list_random_count) + '\nTime to download tweets: ' + str(execution_time) + ' seconds')'''


    def collect_tweets_random_slots_countries_day(self, list_countries, output_file, list_random, max_results, slots_minutes):
        '''
        Collect n tweets in a time range within different random slots (set per each day separately to have a more even distribution)
        and for different countries
        :param list_countries: list of countries from which we retrieve tweets
        :param output_file: output file's name
        :param list_random: random time slots
        :param max_results:  number of tweetsto retrieve
        :param slots_minutes: number of minutes per time slot
        :return: output file with collected tweets
        '''

        # count total number of tweets
        count_tweets = 0
        # count slots
        list_random_count = 0
        # count day
        day = 0
        # count country list
        list_countries_count = 0

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        start = time.time()

        # retrieve desired quantity of tweets
        with open(output_file, 'a+') as file:
            # print('year', list_random[year])

            while count_tweets < max_results:

                query = 'place_country:' + list_countries[list_countries_count]

                if list_random_count < len(list_random[day]):
                    for page in tw.search_all(query=query,
                                              start_time=list_random[day][list_random_count] - datetime.timedelta(minutes=slots_minutes),
                                              end_time=list_random[day][list_random_count]):
                        tweets = expansions.flatten(page)
                        count_tweets_page = 0
                        while count_tweets_page < len(tweets) and count_tweets < max_results:
                            print('Number of tweets retrieved: ', count_tweets)
                            file.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                            count_tweets += 1
                            count_tweets_page += 1
                else:
                    break

                list_countries_count +=1
                # if we went across all the countries
                if list_countries_count == len(list_countries):
                    # if we did all the countries for one slot in one day, start from first country, go to next day,
                    # keep same random slot
                    list_countries_count = 0
                    day += 1
                # if we went across all the days, start again from the first day, update the slot
                if day == 7:
                    list_random_count += 1
                    day = 0

        end = time.time()

        execution_time = end - start

        print('Execution time: ' + str(execution_time))

        '''with open(info_file, 'a+') as file:
            file.write('Number of slots of ' + str(slots_minutes) + ': ' + str(
                list_random_count) + '\nTime to download tweets: ' + str(execution_time) + ' seconds')'''


    def collect_tweets_range(self, query, output_file, info_file, max_results, start_time, end_time):
        '''
        Collect all the tweets in a time range
        :param query: query
        :param output_file: output file
        :param info_file: file with info about execution time
        :param max_results: number of tweets to retrieve
        :param slots_minutes: number of minutes per time slot
        :return: output file with collected tweets
        '''

        # count total number of tweets
        count_tweets = 0

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        start = time.time()
        count = 1
        # retrieve desired quantity of tweets
        with open(output_file, 'a+') as file:
            while count_tweets < max_results:
                for page in tw.search_all(query=query,
                                   start_time=start_time,
                                   end_time=end_time):

                    tweets = expansions.flatten(page)
                    count_tweets_page = 0
                    while count_tweets_page < len(tweets): #and count_tweets < max_results
                        file.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                        count_tweets += 1
                        count_tweets_page += 1

        end = time.time()

        execution_time = end - start

        with open(info_file, 'a+') as file:
            file.write('Time to download tweets: ' + str(execution_time) + ' seconds')


    def collect_json(self, query, output_file, start_time, end_time):
        '''
        Collect tweets in JSON format
        :param query: query
        :param output_file: output in JSON format
        :param start_time: start time to collect tweets
        :param end_time: end time to collect tweets
        :return:
        '''

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        page_count = 0

        for page in tw.search_all(query=query,
                                  start_time=start_time,
                                  end_time=end_time):
            print('ciao')
            with open(output_file+'_'+str(page_count)+'.json', 'w') as file:
                page_count += 1
                json.dump(page, file)


    def collect_tsv_json(self, query, output_tsv, output_json, max_results, start_time, end_time):
        '''
        Collect tweets in tsv and JSON format through a specific query and a time range
        :param query: query
        :param output_tsv: output tsv file
        :param output_json: output json file
        :param max_results: max number of results to retrieve
        :param start_time: start time to collect tweets
        :param end_time: end time to collect tweets
        :return:
        '''

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        count_tweets = 0
        page_count = 0

        with open(output_tsv, 'a+') as file_tsv:

            for page in tw.search_all(query=query,
                                      start_time=start_time,
                                      end_time=end_time):

                if count_tweets < max_results:

                    page_count += 1

                    with open(output_json + '_' + str(page_count) + '.json', 'w') as file_json:
                        json.dump(page, file_json)

                    tweets = expansions.flatten(page)
                    count_tweets_page = 0
                    while count_tweets_page < len(tweets):  # and count_tweets < max_results
                        file_tsv.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                        count_tweets += 1
                        count_tweets_page += 1

                else:
                    break




def main_random(query_str, output_file, info_file):
    '''
    Given the country code, it makes the query to randomly retrieve tweets in certain time range, so it saves them and
    the number of time slots needed to download them
    :param query_str: twitter API query (str)
    :param output_file: output file (str)
    :param info_file: information file (str)
    :return:
    '''

    tweets_collector = Twitter_query()

    # max number of tweets to retrieve
    max_results = 1000000000000000000 #32000

    # slots minutes
    slots_minutes = 2

    list_random_2019 = tweets_collector.create_slots(datetime.datetime(2019, 1, 1, 0, 0, 0), datetime.datetime(2019, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2020 = tweets_collector.create_slots(datetime.datetime(2020, 1, 1, 0, 0, 0), datetime.datetime(2020, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2021 = tweets_collector.create_slots(datetime.datetime(2021, 1, 1, 0, 0, 0), datetime.datetime(2021, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2022 = tweets_collector.create_slots(datetime.datetime(2022, 1, 1, 0, 0, 0), datetime.datetime(2022, 12, 1, 23, 59, 59), slots_minutes)
    list_random = []
    list_random.append(list_random_2019)
    list_random.append(list_random_2020)
    list_random.append(list_random_2021)
    list_random.append(list_random_2022)

    tweets_collector.collect_tweets_random_slots(query_str, output_file, info_file, list_random, max_results, slots_minutes)


def main_random_add(csv_file, query_str, num_tweets_to_reach, output_file, info_file): # num_tweets_to_reach = 31000 for EU and SA
    '''
    Given a dataframe with tweets, it makes additional queries in random slots to populate it
    :param csv_file: csv file with tweets
    :param query_str: query to retrieve tweets
    :param num_tweets_to_reach: number of tweets to reach
    :param output_file: output file
    :param info_file: information file
    :return:
    '''

    # read the csv file with the tweets

    start_df = pd.read_csv(csv_file)

    tweets_to_collect = num_tweets_to_reach - len(start_df)

    tweets_collector = Twitter_query()

    # slots minutes
    slots_minutes = 2

    list_random_2019 = tweets_collector.create_slots(datetime.datetime(2018, 1, 1, 0, 0, 0),
                                                     datetime.datetime(2018, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2020 = tweets_collector.create_slots(datetime.datetime(2017, 1, 1, 0, 0, 0),
                                                     datetime.datetime(2017, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2021 = tweets_collector.create_slots(datetime.datetime(2016, 1, 1, 0, 0, 0),
                                                     datetime.datetime(2016, 12, 31, 23, 59, 59), slots_minutes)

    list_random = []
    list_random.append(list_random_2019)
    list_random.append(list_random_2020)
    list_random.append(list_random_2021)

    tweets_collector.collect_tweets_random_slots(query_str, output_file, info_file, list_random, tweets_to_collect, slots_minutes)


def main_random_countries(list_countries, output_file, max_results, slots_minutes):
    '''
    It creates the time slots and it calls the function 'collect_tweets_random_slots_countries' to retrieve tweets
    :param list_countries: list of countries from which to retrieve tweets
    :param output_file: output file's name
    :param max_results: number of tweets to retrieve
    :param slots_minutes: numbe rof minutes per slot
    :return: file with the collected tweets
    '''

    tweets_collector = Twitter_query()

    list_random_week = tweets_collector.create_slots(datetime.datetime(2023, 5, 15, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 21, 23, 59, 59), slots_minutes)

    tweets_collector.collect_tweets_random_slots_countries(list_countries, output_file, list_random_week, max_results, slots_minutes)


def main_random_countries_day(list_countries, output_file, max_results, slots_minutes):
    '''
    It creates the time slots and it calls the function 'collect_tweets_random_slots_countries_day' to retrieve tweets
    :param list_countries: list of countries from which to retrieve tweets
    :param output_file: output file's name
    :param max_results: number of tweets to retrieve
    :param slots_minutes: numbe rof minutes per slot
    :return: file with the collected tweets
    '''

    tweets_collector = Twitter_query()

    list_random_day1 = tweets_collector.create_slots(datetime.datetime(2023, 5, 15, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 15, 23, 59, 59), slots_minutes)
    list_random_day2 = tweets_collector.create_slots(datetime.datetime(2023, 5, 16, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 16, 23, 59, 59), slots_minutes)
    list_random_day3 = tweets_collector.create_slots(datetime.datetime(2023, 5, 17, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 17, 23, 59, 59), slots_minutes)

    list_random_day4 = tweets_collector.create_slots(datetime.datetime(2023, 5, 18, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 18, 23, 59, 59), slots_minutes)
    list_random_day5 = tweets_collector.create_slots(datetime.datetime(2023, 5, 19, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 19, 23, 59, 59), slots_minutes)
    list_random_day6 = tweets_collector.create_slots(datetime.datetime(2023, 5, 20, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 20, 23, 59, 59), slots_minutes)
    list_random_day7 = tweets_collector.create_slots(datetime.datetime(2023, 5, 21, 0, 0, 0),
                                                     datetime.datetime(2023, 5, 21, 23, 59, 59), slots_minutes)

    list_random = []

    list_random.append(list_random_day1)
    list_random.append(list_random_day2)
    list_random.append(list_random_day3)
    list_random.append(list_random_day4)
    list_random.append(list_random_day5)
    list_random.append(list_random_day6)
    list_random.append(list_random_day7)

    tweets_collector.collect_tweets_random_slots_countries_day(list_countries, output_file, list_random, max_results, slots_minutes)


def main_range(query_str, max_results, output_file, info_file, start_time, end_time):
    '''
    Collect tweets in a time range according to the query
    :param query_str: query
    :param max_results: number of tweets to retrieve
    :param output_file: output file
    :param info_file: file with info about execution time
    :param start_time: start time to collect tweets
    :param end_time: end time to collect tweets
    :return: output file with collected tweets
    '''

    tweets_collector = Twitter_query()

    # max number of tweets to retrieve
    max_results = max_results

    tweets_collector.collect_tweets_range(query_str, output_file, info_file, max_results, start_time, end_time)