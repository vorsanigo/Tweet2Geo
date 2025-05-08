'''
Class to, given the downloaded tweets in a txt format, save them in a csv format
'''

import json
import pandas as pd



class Tweets_dataset():


    def create_df(self, tweets_file, tweets_num):
        '''
        From tweets (txt file with one json tweet per line), create dataset
        :param tweets_file: file with tweets
        :param df: df to create
        :param tweets_num: number of tweets to put into the df
        :return: created df of tweets
        '''

        ids = []
        i = 0

        df_input = pd.read_csv(tweets_file, sep='\t')
        df_input.columns = ['col']

        df = pd.DataFrame(columns=[])

        with open(tweets_file, 'r') as tweets:

            for line in tweets:

                # if we haven't reached the number of tweets we want to put in the df
                if i < tweets_num:

                    try:
                        # load json line
                        tweet_json = json.loads(line)
                        text = None

                        # access all the fields in the json
                        if 'id' in tweet_json.keys():
                            id = int(tweet_json['id'])
                        else:
                            id = None
                        if 'author_id' in tweet_json.keys():
                            author_id = int(tweet_json['author_id'])
                        else:
                            author_id = None
                        if 'author' in tweet_json.keys():
                            author = tweet_json['author']
                            if 'location' in author.keys():
                                location = author['location']
                            else:
                                location = None
                            if 'username' in author.keys():
                                username = author['username']
                            else:
                                username = None
                            if 'public_metrics' in author.keys():
                                if 'followers_count' in author['public_metrics']:
                                    followers = author['public_metrics']['followers_count']
                                else:
                                    followers = None
                            else:
                                followers = None
                            if 'verified' in author:
                                verified = author['verified']
                            else:
                                verified = None
                        else:
                            location = None
                            username = None
                            followers = None
                            verified = None
                        if 'created_at' in tweet_json.keys():
                            created_at = tweet_json['created_at']
                        else:
                            created_at = None
                        if 'lang' in tweet_json.keys():
                            lang = tweet_json['lang']
                        else:
                            lang = None
                        if 'conversation_id' in tweet_json.keys():
                            conversation_id = tweet_json['conversation_id']
                        else:
                            conversation_id = None
                        if 'source' in tweet_json.keys():
                            source = tweet_json['source']
                        else:
                            source = None
                        if 'entities' in tweet_json.keys():
                            if 'mentions' in tweet_json['entities'].keys():
                                mentions = []
                                for m in tweet_json['entities']['mentions']:
                                    mentions.append(int(m['id']))
                            else:
                                mentions = None
                            if 'urls' in tweet_json['entities'].keys():
                                urls = []
                                for u in tweet_json['entities']['urls']:
                                    if 'extended_url' in u.keys():
                                        urls.append(u['extended_url'])
                                    elif 'url' in u.keys():
                                        #urls.append(urlexpander.expand(u['url']))
                                        urls.append(u['url'])
                            else:
                                urls = None
                            if 'hashtags' in tweet_json['entities'].keys():
                                hashtags = []
                                for h in tweet_json['entities']['hashtags']:
                                    hashtags.append(h['tag'])
                            else:
                                hashtags = None
                            if 'annotations' in tweet_json['entities'].keys():
                                annotations = []
                                loc_text = []
                                for n in tweet_json['entities']['annotations']:
                                    if 'type' in n.keys() and 'probability' in n.keys():
                                        if n['type'] == 'Place':
                                            annotations.append((n['normalized_text'], float(n['probability'])))
                                            loc_text.append(n['normalized_text'])
                            else:
                                annotations = None
                                loc_text = None
                        else:
                            mentions = None
                            urls = None
                            hashtags = None
                            annotations = None
                            loc_text = None
                        if 'referenced_tweets' in tweet_json.keys():
                            types = []
                            referenced_tweets_user = []
                            for el in tweet_json['referenced_tweets']:
                                if 'type' in el.keys():
                                    types.append(el['type'])
                                    if el['type'] == 'retweeted':
                                        if 'text' in el.keys():
                                            text = el['text']
                                if 'id' in el.keys() and 'author_id' in el.keys():
                                    referenced_tweets_user.append((int(el['id']), int(el['author_id'])))
                        else:
                            types = None
                            referenced_tweets_user = None
                        if 'public_metrics' in tweet_json.keys():
                            if 'retweet_count' in tweet_json['public_metrics']:
                                retweet_count = tweet_json['public_metrics']['retweet_count']
                            else:
                                retweet_count = None
                            if 'reply_count' in tweet_json['public_metrics']:
                                reply_count = tweet_json['public_metrics']['reply_count']
                            else:
                                reply_count = None
                            if 'like_count' in tweet_json['public_metrics']:
                                like_count = tweet_json['public_metrics']['like_count']
                            else:
                                like_count = None
                            if 'quote_count' in tweet_json['public_metrics']:
                                quote_count = tweet_json['public_metrics']['quote_count']
                            else:
                                quote_count = None
                        else:
                            retweet_count = None
                            reply_count = None
                            like_count = None
                            quote_count = None
                        if text is None:
                            if 'text' in tweet_json.keys():
                                text = tweet_json['text']
                        if 'geo' in tweet_json.keys():
                            if 'place_id' in tweet_json['geo']:
                                place_id = tweet_json['geo']['place_id']
                            else:
                                place_id = None
                            if 'full_name' in tweet_json['geo']:
                                place_name = tweet_json['geo']['full_name']
                            else:
                                place_name = None
                            if 'country_code' in tweet_json['geo']:
                                place_country = tweet_json['geo']['country_code']
                            else:
                                place_country = None
                            if 'geo' in tweet_json['geo']:
                                bbox = tweet_json['geo']['geo']['bbox']
                            else:
                                bbox = None
                        else:
                            place_id = None
                            place_name = None
                            place_country = None
                            bbox = None

                        # avoid duplicate tweets
                        if id not in ids:
                            # append tweet to the df
                            df = df.append({'id': id, 'author_id': author_id, 'location': location, 'username': username,
                                            'created_at': created_at, 'lang': lang, 'text': text, 'loc_text': loc_text, 'types': types,
                                            'referenced_tweets_author': referenced_tweets_user, 'conversation_id': conversation_id,
                                            'source': source, 'hashtags': hashtags, 'mentions': mentions, 'urls': urls,
                                            'annotations_place_prob': annotations, 'followers': followers, 'verified': verified,
                                            'retweet_count': retweet_count, 'reply_count': reply_count, 'like_count': like_count,
                                            'quote_count': quote_count, 'place_id': place_id, 'place_name': place_name,
                                            'place_country': place_country, 'bbox': bbox}, ignore_index=True)
                            ids.append(id)

                            i += 1

                    except:
                        print('Line not loaded')
                        continue
                else:
                    break

        return df


    def add_tweets(self, tweets_file, csv_file, tweets_num):
        '''
        Add tweets (txt file with one json tweet per line) to an already existant dataset (format as the one from the function "create_df")
        :param tweets_file: file with tweets
        :param csv_file: csv of df to add tweets to
        :param tweets_num: number of tweets to put into the df
        :return: df of tweets
        '''

        df = pd.read_csv(csv_file)
        tweet_ids = list(df['id'])

        ids = []
        i = 0
        with open(tweets_file, 'r') as tweets:

            for line in tweets:

                if len(df) < tweets_num:

                    tweet_json = json.loads(line)

                    text = None

                    if 'id' in tweet_json.keys():
                        id = int(tweet_json['id'])
                    else:
                        id = None
                    if 'author_id' in tweet_json.keys():
                        author_id = int(tweet_json['author_id'])
                    else:
                        author_id = None
                    if 'author' in tweet_json.keys():
                        author = tweet_json['author']
                        if 'location' in author.keys():
                            location = author['location']
                        else:
                            location = None
                        if 'username' in author.keys():
                            username = author['username']
                        else:
                            username = None
                        if 'public_metrics' in author.keys():
                            if 'followers_count' in author['public_metrics']:
                                followers = author['public_metrics']['followers_count']
                            else:
                                followers = None
                        else:
                            followers = None
                        if 'verified' in author:
                            verified = author['verified']
                        else:
                            verified = None
                    else:
                        location = None
                        username = None
                        followers = None
                        verified = None
                    if 'created_at' in tweet_json.keys():
                        created_at = tweet_json['created_at']
                    else:
                        created_at = None
                    if 'lang' in tweet_json.keys():
                        lang = tweet_json['lang']
                    else:
                        lang = None
                    if 'conversation_id' in tweet_json.keys():
                        conversation_id = tweet_json['conversation_id']
                    else:
                        conversation_id = None
                    if 'source' in tweet_json.keys():
                        source = tweet_json['source']
                    else:
                        source = None
                    if 'entities' in tweet_json.keys():
                        if 'mentions' in tweet_json['entities'].keys():
                            mentions = []
                            for m in tweet_json['entities']['mentions']:
                                mentions.append(int(m['id']))
                        else:
                            mentions = None
                        if 'urls' in tweet_json['entities'].keys():
                            urls = []
                            for u in tweet_json['entities']['urls']:
                                if 'extended_url' in u.keys():
                                    urls.append(u['extended_url'])
                                elif 'url' in u.keys():
                                    # urls.append(urlexpander.expand(u['url']))
                                    urls.append(u['url'])
                        else:
                            urls = None
                        if 'hashtags' in tweet_json['entities'].keys():
                            hashtags = []
                            for h in tweet_json['entities']['hashtags']:
                                hashtags.append(h['tag'])
                        else:
                            hashtags = None
                        if 'annotations' in tweet_json['entities'].keys():
                            annotations = []
                            loc_text = []
                            for n in tweet_json['entities']['annotations']:
                                if 'type' in n.keys() and 'probability' in n.keys():
                                    if n['type'] == 'Place':
                                        annotations.append((n['normalized_text'], float(n['probability'])))
                                        loc_text.append(n['normalized_text'])
                        else:
                            annotations = None
                            loc_text = None
                    else:
                        mentions = None
                        urls = None
                        hashtags = None
                        annotations = None
                        loc_text = None
                    if 'referenced_tweets' in tweet_json.keys():
                        types = []
                        referenced_tweets_user = []
                        for el in tweet_json['referenced_tweets']:
                            if 'type' in el.keys():
                                types.append(el['type'])
                                if el['type'] == 'retweeted':
                                    if 'text' in el.keys():
                                        text = el['text']
                            if 'id' in el.keys() and 'author_id' in el.keys():
                                referenced_tweets_user.append((int(el['id']), int(el['author_id'])))
                    else:
                        types = None
                        referenced_tweets_user = None
                    if 'public_metrics' in tweet_json.keys():
                        if 'retweet_count' in tweet_json['public_metrics']:
                            retweet_count = tweet_json['public_metrics']['retweet_count']
                        else:
                            retweet_count = None
                        if 'reply_count' in tweet_json['public_metrics']:
                            reply_count = tweet_json['public_metrics']['reply_count']
                        else:
                            reply_count = None
                        if 'like_count' in tweet_json['public_metrics']:
                            like_count = tweet_json['public_metrics']['like_count']
                        else:
                            like_count = None
                        if 'quote_count' in tweet_json['public_metrics']:
                            quote_count = tweet_json['public_metrics']['quote_count']
                        else:
                            quote_count = None
                    else:
                        retweet_count = None
                        reply_count = None
                        like_count = None
                        quote_count = None
                    if text is None:
                        if 'text' in tweet_json.keys():
                            text = tweet_json['text']
                    if 'geo' in tweet_json.keys():
                        if 'place_id' in tweet_json['geo']:
                            place_id = tweet_json['geo']['place_id']
                        else:
                            place_id = None
                        if 'full_name' in tweet_json['geo']:
                            place_name = tweet_json['geo']['full_name']
                        else:
                            place_name = None
                        if 'country_code' in tweet_json['geo']:
                            place_country = tweet_json['geo']['country_code']
                        else:
                            place_country = None
                        if 'geo' in tweet_json['geo']:
                            bbox = tweet_json['geo']['geo']['bbox']
                        else:
                            bbox = None
                    else:
                        place_id = None
                        place_name = None
                        place_country = None
                        bbox = None

                    if id not in ids and id not in tweet_ids:
                        df = df.append({'id': id, 'author_id': author_id, 'location': location, 'username': username,
                                        'created_at': created_at, 'lang': lang, 'text': text, 'loc_text': loc_text,
                                        'types': types,
                                        'referenced_tweets_author': referenced_tweets_user,
                                        'conversation_id': conversation_id,
                                        'source': source, 'hashtags': hashtags, 'mentions': mentions, 'urls': urls,
                                        'annotations_place_prob': annotations, 'followers': followers,
                                        'verified': verified,
                                        'retweet_count': retweet_count, 'reply_count': reply_count,
                                        'like_count': like_count,
                                        'quote_count': quote_count, 'place_id': place_id, 'place_name': place_name,
                                        'place_country': place_country, 'bbox': bbox}, ignore_index=True)
                        ids.append(id)

                        i += 1

                else:
                    break

        return df