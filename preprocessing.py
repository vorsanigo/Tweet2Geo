import pandas as pd
import emoji
import re
import demoji
import requests
import json



def remove_accents(name):
    '''
    Given a string, it removes the accents
    :param name: word str
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
    :param my_string: string
    :param weird_words: weird words list
    :return:
    '''

    #my_string = unidecode.unidecode(my_string)  # eliminate weird accents
    #my_string = remove_accents(my_string)
    #my_string = re.sub(r'[^a-zA-Z0-9А-яء-ي]+', ' ', my_string)  # eliminate what is not letter or number

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
    :param my_string: string
    :param list_flags: list of flags
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


def cleaning_nominatim(starting_location, list_flags, weird_words):
    '''
    Given a starting location, a list of flags, and a list of weird words, it returns the cleaned location
    :param starting_location: string of sarting location
    :param list_flags: list of flags
    :param weird_words: list of weird words
    :return: cleaned location
    '''

    cleaned_loc = transform_emoji(starting_location, list_flags)
    cleaned_loc = remove_special(cleaned_loc, weird_words)

    return cleaned_loc



def cleaning(location_dataset, unicodes_flags_dataset, weird_words_dataset, output_file):
    '''
    Given the dataset with locations, the flags unicodes, the weird words, it does the locations cleaning
    :param location_dataset: path of dataset of locations
    :param unicodes_flags_dataset: path of dataset of unicodes flags
    :param weird_words_dataset: path of dataset of weird words
    :param output_file: path of output file
    :return: cleaned dataset of locations
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
    :param my_string: location string
    :param list_flags: list of flags
    :return: cleaned string
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
    :param text: text
    :return: cleaned text
    '''

    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', text)
    text = re.sub(r'#([^\s]+)', r'\1', text)

    return text


def replace_mention(text):
   '''
   It replaces mentions with "_MENTION_"
   :param text: text
   :return: text with mentions replaced with "_MENTION_"
   '''

   return re.sub("@[A-Za-z0-9_]+", "_MENTION_", text)


def find_hashtag(text):
    '''
    It returns hashtags in a tweet
    :param text: text
    :return: text splitted where there are hashtags
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
    :param text: text
    :param list_flags: list of flags
    :return: cleaned text
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









# Not used

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