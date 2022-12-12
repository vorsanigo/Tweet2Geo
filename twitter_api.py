# how to make queries -> https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
# twarc -> https://github.com/DocNow/twarc | https://github.com/DocNow/twarc/tree/main/twarc

# 6 years -> divide into ranges of 15 minutes, take avg time to have 30k tweets, divide it by 96 (1440/15) to have the
# number of time ranges of 15 min, take randomly that number of time ranges, add 15 minutes to each of them to have start
# time and end time
# download, save tweets in those time ranges until we have 30k tweets
# consider fields of interest for us and put them in a dataset


from twarc import Twarc2, expansions
import datetime
import time
import random
import json


API_KEY = "qnB2yT0gIDuzimWxrw3lpir14"
API_KEY_SECRET = "EpiWl4qmpADUEp2TPveKIXQYR5ncWuZqApJYNsW27GkVq1JrAm"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAHGhbQEAAAAAnY1dM1zIBEQucG%2F2Aij%2Bx8ksoR0%3DVAdbIz5Hk43FAdwvsCfNJX9QyhEcQKuDmpjftVSEvV0IsZjMWF"
ACCESSEN_TOKEN = "1509526212959354889-FUZhKPYqGhHdVOmMRddvoWbW8vCj88"
TOKEN_SECRET = "wKT3pakdFNM1XuSDhjtZVDYzcAWjd7gin9vN5DfOw7MI0"



class Twitter_query():


    def create_slots(self, start_date, end_date, minutes):
        '''
        Create slots of time of n minutes between a start and end date
        :param start_date: datetime.datetime()
        :param end_date: datetime.datetime()
        :param minutes: int
        :return: list of times within the selected dates
        '''
        # create slots of 15 minutes
        mintime = int(time.mktime(start_date.timetuple()))
        maxtime = int(time.mktime(end_date.timetuple()))
        nb_slots = (maxtime - mintime)//(minutes*60)
        print('s', nb_slots)
        # slots list
        list_random = []
        for RECORD in range(nb_slots):
            random_slot = random.randint(0, nb_slots)
            random_ts = mintime + minutes * 60 * random_slot
            random_datetime = datetime.datetime.fromtimestamp(random_ts)
            list_random.append(random_datetime)

        return list_random



    def collect_tweets_date(self, query, output_file, info_file, list_random, max_results, slots_minutes):
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
        print(list_random[0][3])
        tw = Twarc2(bearer_token=BEARER_TOKEN)

        # retrieve desired quantity of tweets
        with open(output_file, 'a+') as file:
            while count_tweets < max_results:
                print('ccccccccccccccc')
                if list_random_count < len(list_random[year]):
                    print('dddddddddddddddddddddddddd')
                    print(list_random[year][list_random_count])
                    for page in tw.search_all(query=query,
                                           start_time=list_random[year][list_random_count]-datetime.timedelta(minutes=slots_minutes),
                                           end_time=list_random[year][list_random_count]):
                        print('zzzzzzzzzzzzzzzzz')
                        print('\n\n')
                        tweets = expansions.flatten(page)
                        count_tweets_page = 0
                        while count_tweets_page < len(tweets) and count_tweets < max_results:
                            print('ellll', tweets[count_tweets_page])
                            print(count_tweets)
                            file.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                            count_tweets += 1
                            count_tweets_page += 1
                else:
                    break
                year += 1
                if year == 4:
                    list_random_count += 1
                    year = 0

        with open(info_file, 'a+') as file:
            file.write('Number of slots of ' + str(slots_minutes) + ': ' + str(list_random_count))



def main():

    tweets_collector = Twitter_query()

    # query
    query = 'place_country:BG'
    # max number of tweets to retrieve
    max_results = 30000
    # slots minutes
    slots_minutes = 2
    # output file
    output_file = 'BG1'
    # info file
    info_file = 'BG1_info'

    list_random_2019 = tweets_collector.create_slots(datetime.datetime(2019, 1, 1, 0, 0, 0), datetime.datetime(2019, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2020 = tweets_collector.create_slots(datetime.datetime(2020, 1, 1, 0, 0, 0), datetime.datetime(2020, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2021 = tweets_collector.create_slots(datetime.datetime(2021, 1, 1, 0, 0, 0), datetime.datetime(2021, 12, 31, 23, 59, 59), slots_minutes)
    list_random_2022 = tweets_collector.create_slots(datetime.datetime(2022, 1, 1, 0, 0, 0), datetime.datetime(2022, 12, 1, 23, 59, 59), slots_minutes)
    list_random = []
    list_random.append(list_random_2019)
    list_random.append(list_random_2020)
    list_random.append(list_random_2021)
    list_random.append(list_random_2022)
    tweets_collector.collect_tweets_date(query, output_file, info_file, list_random, max_results, slots_minutes)


if __name__ == '__main__':
    main()