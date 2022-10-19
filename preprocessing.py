import pandas as pd
import emoji
import re
import demoji
import requests
import json



def remove_accents(name):
    '''
    Given a string, it removes the accents
    :param name:
    :return:
    '''

    name = re.sub('á','a', name)
    name = re.sub('é','e', name)
    name = re.sub('í','i', name)
    name = re.sub('ó','o', name)
    name = re.sub('ú','u', name)
    name = re.sub('ý','y', name)
    name = re.sub('à','a', name)
    name = re.sub('è','e', name)
    name = re.sub('ì','i', name)
    name = re.sub('ò','o', name)
    name = re.sub('ù','u', name)
    name = re.sub('ä','a', name)
    name = re.sub('ë','e', name)
    name = re.sub('ï','i', name)
    name = re.sub('ö','o', name)
    name = re.sub('ü','u', name)
    name = re.sub('ÿ','y', name)
    name = re.sub('â','a', name)
    name = re.sub('ê','e', name)
    name = re.sub('î','i', name)
    name = re.sub('ô','o', name)
    name = re.sub('û','u', name)
    name = re.sub('ã','a', name)
    name = re.sub('õ','o', name)
    name = re.sub('ñ','n', name)

    return name


def remove_special(my_string, weird_words):
    '''
    Remove special characters (non alphanumeric) and weird words
    :param my_string:
    :param weird_words:
    :return:
    '''

    #my_string = unidecode.unidecode(my_string)  # eliminate weird accents
    #my_string = remove_accents(my_string)
    #my_string = re.sub(r'[^a-zA-Z0-9А-яء-ي]+', ' ', my_string)  # eliminate what is not letter or number

    #my_string = remove_accents(my_string)

    my_string = my_string.lower()
    for c in my_string:
        if not (c.isalpha() or c.isdigit() or c == ' '):
            my_string = my_string.replace(c, ' ')
    my_string = re.sub(' +', ' ', my_string)
    for word in weird_words: # TODO decidere come toglierle e queli -> cosa fare se sono sottostringhe di parole? se servissero?
        if word in my_string:
            my_string = my_string.replace(word, ' ')
    my_string = re.sub(' +', ' ', my_string)
    my_string = my_string.strip()
    if my_string == ' ':
        my_string = my_string.replace(' ', '')

    return my_string


def transform_emoji(my_string, list_flags):
    '''
    Keep only flags emojis, remove others
    :param my_string:
    :param list_flags:
    :return:
    '''

    # run only once after installing module
    #demoji.download_codes()

    em = demoji.findall(my_string)

    for e in em:
        if em[e] in (list_flags):
            my_string = my_string.replace(e, ' ' + em[e][6:] + ' ')
        else:
            my_string = my_string.replace(e, ' ')

    return my_string


def cleaning_country(country):
    '''
    Given a country, it removes its accents and put to lower case match with locations
    :param country:
    :return:
    '''

    country_1 = remove_accents(country)
    country_2 = country_1.lower()

    return country_2

def cleaning_country_tot(file_countries, list_lang):
    '''
    Given the df of countries, it returns the df with cleaned countries
    :param file_countries:
    :param list_lang:
    :return:
    '''

    df = pd.read_csv(file_countries)

    for el in list_lang:
        df[el] = df[el].apply(lambda x: cleaning_country(x))

    df.to_csv('./world_countries_cleaned.csv', sep = '\t')

    return df

#print(cleaning_country_tot('world.csv', ['ar', 'bg', 'cs', 'da', 'de', 'el', 'en', 'eo', 'es', 'et', 'eu', 'fi', 'fr', 'hu', 'it', 'ja', 'ko', 'lt', 'nl', 'no', 'pl', 'pt', 'ro', 'ru', 'sk', 'sv', 'th', 'uk', 'zh', 'zh-tw']))


def cleaning_nominatim(starting_location, list_flags, weird_words):

    cleaned_loc = transform_emoji(starting_location, list_flags)
    cleaned_loc = remove_special(cleaned_loc, weird_words)

    return cleaned_loc



def cleaning(location_dataset, unicodes_flags_dataset, weird_words_dataset, output_file):
    '''
    Given the dataset with locations, the flags unicodes, the weird words, it does the locations cleaning
    :param location_dataset:
    :param unicodes_flags_dataset:
    :param weird_words_dataset:
    :param output_file:
    :return:
    '''

    df_unicodes = pd.read_csv(unicodes_flags_dataset, sep='\t') # TODO put ue flag in the list
    list_flags = list(df_unicodes['country_code'])

    df_loc = pd.read_csv(location_dataset, sep='\t')

    df_weird_words = pd.read_csv(weird_words_dataset)
    weird_words = list(df_weird_words['word'])

    cleaned_localities = []

    for loc in df_loc['location'].values:
        if not pd.isna(loc):
            loc = transform_emoji(loc, list_flags)
            loc = remove_special(loc, weird_words)
        cleaned_localities.append(loc)

    df_loc['cleaned_loc'] = cleaned_localities

    df_loc.to_csv(output_file, sep='\t', index=False)

    return df_loc


def cleaning_ner_loc(my_string, list_flags):
    '''
    It computes cleaning on tweet field location for NER
    :param my_string:
    :param list_flags:
    :return:
    '''

    my_string = transform_emoji(my_string, list_flags)
    my_string = my_string.replace('|', ',')
    my_string = re.sub(' +', ' ', my_string)
    my_string = my_string.strip()
    if my_string == ' ':
        my_string = my_string.replace(' ', '')

    return my_string


def replace_URL(text):
    '''
    It replaces url address with "URL"
    :param text:
    :return:
    '''

    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', text)
    text = re.sub(r'#([^\s]+)', r'\1', text)

    return text


def replace_mention(text):
   '''
   It replaces mentions with "_MENTION_"
   :param text:
   :return:
   '''

   return re.sub("@[A-Za-z0-9_]+", "_MENTION_", text)


def find_hashtag(text):
    '''
    It returns hashtags in a tweet
    :param text:
    :return:
    '''

    return set([el for el in text.split() if el.startswith("#")])


def preprocess_tweet_text(text, lang): # it puts everything to lower case -> we use this only on hshtags
    '''
    It preprocesses the tweet text, we use it only on hastags (it divides words in hashtags) after havin extracted them
    since we want to keep the text as given to extract Named Entities
    :param text:
    :param lang:
    :return:
    '''

    # todo check possible len
    url = "https://dh.fbk.eu/dh-ppp"
    headers = {"Content-Type": "text/json"}

    text = text.strip("\n")
    parts = text.split("\t")
    dataDict = dict()
    dataDict["text"] = parts[0]
    dataDict["lang"] = 'en' #lang  # sono disponibili circa 10 lingue ma non per tutte c'è la lemmatizzazione nella parte delle emozioni
    r = requests.post(url, data=json.dumps(dataDict), headers=headers)
    a = r.json()

    return a['preprocessedText']


def clean_tweet_text(text, list_flags, tweet_lang):
    '''
    It computes cleaning on tweet text (split hashtags, transform flags emoji, replace URLs, replace mentions)
    :param text:
    :param list_flags:
    :return:
    '''

    dict_hashtags = find_hashtag(text)
    for el in dict_hashtags:
        text = text.replace(el, preprocess_tweet_text(el, tweet_lang))
    text = transform_emoji(text, list_flags)
    text = replace_URL(text)
    text = replace_mention(text)
    text = re.sub(' +', ' ', text)
    text = text.strip()

    return text




def dataframe_chunk(df, chunk_size):
    '''
    Function to divide dataset into chunks
    Takes as input:
    - df: the dataframe
    - chunk_size: the size of the chunk
    '''

    num_chunks = len(df) // chunk_size
    if len(df) % chunk_size != 0:
        num_chunks += 1
    for i in range(num_chunks):
        yield df[i*chunk_size:(i + 1) * chunk_size]



# not used
def split_hashtag(text):
    '''
    It removes '#' and splits hashtags words
    :param text:
    :return:
    '''
    # todo -> deal with hashtags with no capital letters
    return re.sub(r'#[a-z]\S*|#[A-Z]\S*', lambda m: ' '.join(re.findall('[A-Z][^A-Z]*|[a-z][^A-Z]*', m.group().lstrip('#'))), text,)

    # note:
    # il campo preprocessEmotions: ritorna il testo con il seguente preprocessing: lemmatizzazione (con Spacy, volendo migliorabile usando Tint), senza stopwords, hashtags-split, users-url etc normaliztion
    # il campo preprocessedText: ritorna il testo con il seguente preprocessing:  hashtags-split, users-url etc normaliztion
    # colonne emozioni: 'nrcDict_Anger', 'nrcDict_Anticipation', 'nrcDict_Disgust', 'nrcDict_Fear', 'nrcDict_Joy', 'nrcDict_Sadness', 'nrcDict_Surprise', 'nrcDict_Trust', 'nrcVadDict_Valence','nrcVadDict_Arousal', 'nrcVadDict_Dominance','nrcPosNegDict_Positive','nrcPosNegDict_Negative'
    # altre opzioni disponibili da passare "emoji":"yes","emotions":"yes" (or "no"). Emotions = no fa solo il primo prepreocessing. Emoji = no, nn trasforma le emoji
    # test rapido con curl:
    # curl --header "Content-Type: text/json" --request POST --data '{"text":"I love everyone #blacklivesmatter 🙂","lang":"en","emoji":"yes","emotions":"yes"}' https://dh.fbk.eu/dh-ppp

#print(clean_tweet_text_("Mi fa paura chi \u00e8 sempre certo di ogni cosa...fatevelo venire un dubbio ogni tanto\nCos\u00ec, perch\u00e9 fa bene provare a vedere le cose da altre prospettive #ebbidubbisempre #certezzerelative", 'unicode_flags.csv'))
#print(clean_tweet_text("@borghi_claudio @valy_s @ZombieBuster5 @Marinojump @cris_cersei @OrtigiaP @maryfagi @Rinaldi_euro @LykanLA @matteosalvinimi @armandosiri @Capezzone @molumbe @Michele_Arnese @AStramezzi @a_meluzzi @RadioRadioWeb @Black300Joe @BarbaraRaval @lameduck1960 @Yi_Benevolence @CriticaScient @fdragoni @Storace siri \u00e8 un uomo buono generoso con tutti sono orgogliosa di lui non deve mollare perch\u00e8 noi abbiamo bisogno di persone come lui #combattere \u2764 https://pbs.twimg.com/profile_images/1411439122317811714/TXzm_HR4_normal.jpg ciao come stai @nasaryen a sensata", 'unicode_flags.csv'))

#preprocess_tweet_text('Mi fa paura chi \u00e8 sempre certo di ogni cosa...fatevelo venire un dubbio ogni tanto\nCos\u00ec, perch\u00e9 fa bene provare a vedere le cose da altre prospettive #ebbidubbisempre #certezzerelative @Rinaldi_euro @LykanLA @matteosalvinimi')
#df = cleaning('user_loc_more_info.tsv', 'unicode_flags.csv', 'weird words 1.csv', 'tot_cleaned_more_info.csv')
#df = cleaning('user_loc_italy.tsv', 'unicode_flags.csv', 'weird words 1.csv', 'tot_cleaned_italy.csv')
#df = cleaning('user_loc_more_info_no retweets quotes.tsv', 'unicode_flags.csv', 'weird words 1.csv', 'tot_cleaned_more_info_no retweets quotes.csv')


#df = cleaning('./650_world.csv', 'unicode_flags.csv', 'weird words 1.csv', '650_world_cleaned.csv')

#df = cleaning('./650_world.csv', 'unicode_flags.csv', 'weird words 1.csv', 'cuccuruccu.csv')


'''df = pd.read_csv('eval ner text total/text_loc_eval.csv', sep='\t')
df['cleaned_text'] = pd.NA
fl = pd.read_csv('unicode_flags.csv', sep='\t')
list_flags = list(fl['country_code'])
for i in range(len(df)):
    df.at[i, 'cleaned_text'] = clean_tweet_text(df.at[i, 'text'], list_flags, df.at[i, 'lang'])
df.to_csv('text_loc_eval (copy)_0.csv')'''

























# NOT USEFUL


def find_emoji_flags_1(string, unicodes_emoji_flags_df):
    #print(type(string))
    #s = str(string.encode('ascii', 'backslashreplace'))
    '''s = s.replace(' ', '\\\\')
    print(s)
    print(type(s))
    s1 = s.split('\\\\')
    print(s1)'''
    '''n = 2
    unigrams = ngrams(re.split('\\\\|, ', s), n)
    print(re.split('\\\\|, |,', s))
    #unigrams = ngrams(s.split('\\'), n)

    for item in unigrams:
        print(item)'''

    #df = pd.read_csv(unicodes_emoji_flags_df, sep='\t')
    #print(df)
    '''print(df['unicode'].values)'''
    unicodes = unicodes_emoji_flags_df['unicode'].values
    #print(unicodes)
    #print('eeeeeeeeeeee', unicodes[1])

    emoji_code = (string.encode('ascii', "backslashreplace"))
    '''for code in ["\\\\u0001f3f4\\\\u000e0067\\\\u000e0062\\\\u000e0065\\\\u000e006e\\\\u000e0067\\\\u000e007f",
                 "\\\\u0001f3f4\\\\u000e0067\\\\u000e0062\\\\u000e0073\\\\u000e0063\\\\u000e0074\\\\u000e007f",
                 "\\\\u0001f3f4\\\\u000e0067\\\\u000e0062\\\\u000e0077\\\\u000e006c\\\\u000e0073\\\\u000e007f"]:'''
    '''for code in ["\\\\U0001f3f4\\\\U000e0067\\\\U000e0062\\\\U000e0065\\\\U000e006e\\\\U000e0067\\\\U000e007f",
                 "\\\\U0001f3F4\\\\U000e0067\\\\U000e0062\\\\U000e0073\\\\U000e0063\\\\U000e0074\\\\U000e007f",
                 "\\\\U0001f3f4\\\\U000e0067\\\\U000e0062\\\\U000e0077\\\\U000e006c\\\\U000e0073\\\\U000e007f"]:'''
    for code in ["\\\\U0001f3f4\\\\U000e0067\\\\U000e0062\\\\U000e0065\\\\U000e006e\\\\U000e0067\\\\U000e007f",
                 "\\\\U0001f3F4\\\\U000e0067\\\\U000e0062\\\\U000e0073\\\\U000e0063\\\\U000e0074\\\\U000e007f",
                 "\\\\U0001f3f4\\\\U000e0067\\\\U000e0062\\\\U000e0077\\\\U000e006c\\\\U000e0073\\\\U000e007f"]:
        print('emojiiii', emoji_code)
        print(emoji_code.decode("utf-8"))
        print(type(emoji_code))
        print('codeeee', code)
        print(type(code))
        if code in emoji_code:
            print("ciaooooooooooooooooooooooooo")
            print('code', code)
            print(emoji.demojize(code))
            print(emoji.demojize("\U0001F3F4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f"))
            emoji_code = emoji_code.replace(code, emoji.demojize(code))
            print('new', emoji_code)


    for pos, c in enumerate(string):


        #emoji_code = str(c.encode('ascii', "backslashreplace"))
        #print(emoji_code)
        if c in emoji.EMOJI_DATA:
            print('1', c)
            #emoji_code = str(c.encode('ascii',"backslashreplace"))
            #print(emoji_code)
            #if emoji_code <= "\U0001F1E6\U0001F1E8".lower() or emoji_code >= "\U0001F1FF\U0001F1FC".lower():
            string = string.replace(c, ' ')
            print('1', string)
            #else:
                #print('ciao')

            #string = string.replace(c, emoji.demojize(c))

        if pos < len(string) - 1:# and c+string[pos+1].lower()[1:] in unicodes: #emoji.UNICODE_EMOJI_ENGLISH
            '''print("ciao")
            print(c)
            print(string[pos+1])
            print(c+string[pos+1])'''
            pair_c = c+string[pos+1]
            emoji_code = str(pair_c.encode('ascii', "backslashreplace"))
            #print(emoji_code)
            #print(type(emoji_code))

            #emoji_code = emoji_code.replace('\\\\', '\\')
            #print(emoji_code.upper())
            #if emoji_code >= "\U0001F1E6\U0001F1E8".lower() and emoji_code <= "\U0001F1FF\U0001F1FC".lower():
            #string = string.replace(pair_c, ' ')
                #print('no')
            #else:
                #print('ciao')
            #print(emoji_code.lower()[2:-1])
            if emoji_code.lower()[2:-1] in unicodes:
                string = string.replace(pair_c, emoji.demojize(pair_c))
            print('2', string)
            #print(string)


    return string


def find_emoji_flags(string, unicodes_emoji_flags_df):
    #print(type(string))
    #s = str(string.encode('ascii', 'backslashreplace'))
    '''s = s.replace(' ', '\\\\')
    print(s)
    print(type(s))
    s1 = s.split('\\\\')
    print(s1)'''
    '''n = 2
    unigrams = ngrams(re.split('\\\\|, ', s), n)
    print(re.split('\\\\|, |,', s))
    #unigrams = ngrams(s.split('\\'), n)

    for item in unigrams:
        print(item)'''

    #df = pd.read_csv(unicodes_emoji_flags_df, sep='\t')
    #print(df)
    '''print(df['unicode'].values)'''
    unicodes = unicodes_emoji_flags_df['unicode_1'].values


    for pos, c in enumerate(string):

        '''print("string", string)
        print('kkkkkkkkkkkkkkkkkkkkkkk', c)
        emoji_code = str(c.encode('ascii', "backslashreplace"))
        print('eeeeeeeeeeeee', emoji_code)
        if c in emoji.EMOJI_DATA:
            print('1', c)
            #emoji_code = str(c.encode('ascii',"backslashreplace"))
            #print(emoji_code)
            #if emoji_code <= "\U0001F1E6\U0001F1E8".lower() or emoji_code >= "\U0001F1FF\U0001F1FC".lower():
            string = string.replace(c, ' ')
            print('1', string)'''
            #else:
                #print('ciao')

            #string = string.replace(c, emoji.demojize(c))


        if pos < len(string) - 1:  # and c+string[pos+1].lower()[1:] in unicodes: #emoji.UNICODE_EMOJI_ENGLISH
            '''print("ciao")
            print(c)
            print(string[pos+1])
            print(c+string[pos+1])'''
            pair_c = c + string[pos + 1]
            emoji_code = pair_c.encode('ascii', "backslashreplace").decode("utf-8")
            print('qqq', emoji_code)
            '''emoji_code = str(pair_c.encode('ascii', "backslashreplace"))
            print('eee', emoji_code)'''
            # print(emoji_code)
            # print(type(emoji_code))

            # emoji_code = emoji_code.replace('\\\\', '\\')
            # print(emoji_code.upper())
            # if emoji_code >= "\U0001F1E6\U0001F1E8".lower() and emoji_code <= "\U0001F1FF\U0001F1FC".lower():
            # string = string.replace(pair_c, ' ')
            # print('no')
            # else:
            # print('ciao')
            # print(emoji_code.lower()[2:-1])

            if emoji_code.lower() in unicodes:
                string = string.replace(pair_c, emoji.demojize(pair_c))
            '''if emoji_code.lower()[2:-1] in unicodes:
                string = string.replace(pair_c, emoji.demojize(pair_c))'''
            print('2', string)
            # print(string)

    emoji_code = (string.encode('ascii', "backslashreplace")).decode("utf-8")

    dict_code = {"\\U0001f3f4\\U000e0067\\U000e0062\\U000e0065\\U000e006e\\U000e0067\\U000e007f": '🏴󠁧󠁢󠁥󠁮󠁧󠁿',
                 "\\U0001f3F4\\U000e0067\\U000e0062\\U000e0073\\U000e0063\\U000e0074\\U000e007f": '󠁧󠁢󠁳󠁣󠁴󠁿󠁧󠁢󠁳󠁣󠁴󠁿🏴󠁧󠁢󠁳󠁣󠁴󠁿',
                 "\\U0001f3f4\\U000e0067\\U000e0062\\U000e0077\\U000e006c\\U000e0073\\U000e007f": '🏴󠁧󠁢󠁷󠁬󠁳󠁿'}
    '''for code in ["\\\\u0001f3f4\\\\u000e0067\\\\u000e0062\\\\u000e0065\\\\u000e006e\\\\u000e0067\\\\u000e007f",
                 "\\\\u0001f3f4\\\\u000e0067\\\\u000e0062\\\\u000e0073\\\\u000e0063\\\\u000e0074\\\\u000e007f",
                 "\\\\u0001f3f4\\\\u000e0067\\\\u000e0062\\\\u000e0077\\\\u000e006c\\\\u000e0073\\\\u000e007f"]:'''
    '''for code in ["\\\\U0001f3f4\\\\U000e0067\\\\U000e0062\\\\U000e0065\\\\U000e006e\\\\U000e0067\\\\U000e007f",
                 "\\\\U0001f3F4\\\\U000e0067\\\\U000e0062\\\\U000e0073\\\\U000e0063\\\\U000e0074\\\\U000e007f",
                 "\\\\U0001f3f4\\\\U000e0067\\\\U000e0062\\\\U000e0077\\\\U000e006c\\\\U000e0073\\\\U000e007f"]:'''
    for code in dict_code:
    #for code in [b'\U0001f3f4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f',
    #             b'\U0001f3F4\U000e0067\U000e0062\U000e0073\U000e0063\U000e0074\U000e007f',
    #             b'\U0001f3f4\U000e0067\U000e0062\U000e0077\U000e006c\U000e0073\U000e007f']:
        print('emojiiii', emoji_code)
        #print(emoji_code.decode("utf-8"))
        print(type(emoji_code))
        print('codeeee', code)
        print(type(code))
        if code in emoji_code:
            print("\n\n\n")
            print("ciaooooooooooooooooooooooooo")
            print('code', code)
            print(emoji.demojize(dict_code[code]))
            print(type("\U0001F3F4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f"))
            print(emoji.demojize("\U0001F3F4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f"))
            emoji_code = emoji_code.replace(code, emoji.demojize(dict_code[code]))
            print('new', emoji_code)

    return emoji_code #string
