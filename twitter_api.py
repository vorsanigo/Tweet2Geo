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
        # slots list
        list_random = []
        for RECORD in range(100):
            random_slot = random.randint(0, nb_slots)
            random_ts = mintime + minutes * 60 * random_slot
            random_datetime = datetime.datetime.fromtimestamp(random_ts)
            list_random.append(random_datetime)

        return list_random



    def collect_tweets_date(self, query, output_file, list_random, max_results, slots_minutes):
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

        tw = Twarc2(bearer_token=BEARER_TOKEN)

        # retrieve desired quantity of tweets
        with open(output_file, 'a+') as file:
            while count_tweets < max_results:
                if list_random_count < len(list_random):
                    for page in tw.search_all(query=query,
                                           start_time=list_random[list_random_count]-datetime.timedelta(minutes=slots_minutes),
                                           end_time=list_random[list_random_count]):
                        print('\n\n')
                        tweets = expansions.flatten(page)
                        count_tweets_page = 0
                        while count_tweets_page < len(tweets) and count_tweets < max_results:
                            print('ellll', tweets[count_tweets_page])
                            print(count_tweets)
                            file.write('%s\n' % json.dumps(tweets[count_tweets_page]))
                            count_tweets += 1
                            count_tweets_page += 1
                list_random_count += 1


def main():

    tweets_collector = Twitter_query()

    # query
    query = 'place_country:IT'
    # start date
    start_date = datetime.datetime(2017, 1, 1, 0, 0, 0)
    # end_date
    end_date = datetime.datetime(2022, 12, 1, 0, 0, 0)
    # max number of tweets to retrieve
    max_results = 300
    # slots minutes
    slots_minutes = 15
    # output file
    output_file = 'output.txt'

    list_random = tweets_collector.create_slots(start_date, end_date, slots_minutes)
    tweets_collector.collect_tweets_date(query, output_file, list_random, max_results, slots_minutes)


if __name__ == '__main__':
    main()