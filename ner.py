import pandas as pd
import logging
from tqdm import tqdm
import stanza
from stanza.server import CoreNLPClient
from stanza.pipeline.core import DownloadMethod
import polyglot
from polyglot.text import Text, Word
import csv
from preprocessing import cleaning_ner_loc


#stanza.download_corenlp_models(model='italian', version='4.2.2', dir="~/stanza_corenlp")


# todo NB: PROBLEM -> IF YOU LOAD THE STNAZA MODELS ALL TOGETHER -> MEMORY ISSUES



class NERLocation():
    '''

    '''

    def __init__(self, pretokenized = False,
                        client = CoreNLPClient(
                                annotators=['tokenize', 'ner'],
                                memory='4G',
                                endpoint='http://localhost:8000',
                                be_quiet=True)
                 ):
        self.client = client
        self.pretokenized = pretokenized


    def detect_lang(self, text, lang):
        '''
        Detect the language of the text of the tweet if not available
        :param text: text
        :param lang: language field of the tweet, it represents the text language suggested by Twitter
        :return: the language from Twitter if available, otherwise the detected language
        '''

        '''if pd.isna(lang) or lang == 'und':
            text_ner = Text(text)
            print("Language Detected: Code={}, Name={}\n".format(text_ner.language.code, text_ner.language.name))
            lang = text_ner.language.code

        return lang'''
        return 'en' #we can directly set this


    # corenlp
    # https://github.com/stanfordnlp/CoreNLP/tree/main/src/edu/stanford/nlp/pipeline
    # https://stanfordnlp.github.io/CoreNLP/ner.html
    def ner_coreNLP(self, document, user, write_list, list_entities_loc, org):
        '''
        Detect locations entities from a document (text) using coreNLP
        :param document: text
        :param user: user id (for Twitter)
        :param write_list: it contains user id, the word, the type of entity related to the word
        :param list_entities_loc: list of location entities
        :param org: True/False if we consider also organizations as locations or not
        :return: write_list, list_entities_loc
        '''

        for i, sent in enumerate(document.sentence):
            print("[Sentence {}]".format(i + 1))
            for t in sent.token:
                print("{:12s}\t{}".format(t.word, t.ner))
                print("")
                write_list.append([user, t.word, t.ner])
        for sent in document.sentence:
            for m in sent.mentions:
                print("{:30s}\t{}".format(m.entityMentionText, m.entityType))
                if org == True:
                    if m.entityType == 'LOC' or m.entityType == 'LOCATION' or m.entityType == 'GPE' or m.entityType == \
                            'ORG' or m.entityType == 'ORGANIZATION':
                        list_entities_loc.append(m.entityMentionText)
                else:
                    if m.entityType == 'LOC' or m.entityType == 'LOCATION' or m.entityType == 'GPE':
                        list_entities_loc.append(m.entityMentionText)

        return write_list, list_entities_loc


    # stanza
    # https://stanfordnlp.github.io/stanza/available_models.html
    def ner_stanza(self, document, user, list_entities_loc, dict_entities_tot, write_list):
        '''
        Detect locations entities from a document (text) using stanza
        :param document: text
        :param user: user id (for Twitter)
        :param write_list: it contains user id, the word, the type of entity related to the word
        :param list_entities_loc: list of location entities
        :param org: True/False if we consider also organizations as locations or not
        :return: write_list, list_entities_loc
        '''

        for sentence in document.sentences:
            if sentence.ents != []:
                for entity in sentence.ents:
                    if entity.type == 'LOC' or entity.type == 'LOCATION' or entity.type == 'GPE':
                        list_entities_loc.append(entity.text)
                    dict_entities_tot[entity.text.lower()] = entity.type
                for token in sentence.tokens:
                    if token.text.lower() in dict_entities_tot:
                        write_list.append([user, token.text, dict_entities_tot[token.text.lower()]])
                    else:
                        write_list.append([user, token.text, 0])
            else:
                for token in sentence.tokens:
                    write_list.append([user, token.text, 0])

        return write_list, list_entities_loc


    def ner(self, text, client, lang, user, lang_corenlp, lang_stanza, org):
        '''
        Detect locations entities from text combining coreNLP and stanza (if text language not supported by coreNLP use
        stanza, is also stanza does not support that language use English)
        :param text: text
        :param client: coreNLP client
        :param lang: text language
        :param user: user id (for Twitter)
        :param lang_corenlp: possible languages in coreNLP
        :param lang_stanza: possible languages in stanza
        :param org: True/False if we also consider organizations as locations or not
        :return: list of locations entities, to_write list (user id, word, type of entity related to the word)
        '''

        list_entities_loc = []
        dict_entities_tot = {}
        write_list = []

        processors = 'tokenize, ner'
        dict_nlp_stanza = {}

        lang = self.detect_lang(text, lang)
        print('\n')
        print('langggg', lang)

        if lang in lang_corenlp:
            print('Use coreNLP')
            document = client.annotate(text, properties=lang_corenlp[lang]) # from dict of properties
            write_list, list_entities_loc = self.ner_coreNLP(document, user, write_list, list_entities_loc, org)

        elif lang in lang_stanza:
            print('use stanza')
            if lang in dict_nlp_stanza:
                nlp = dict_nlp_stanza[lang]
            else:
                dict_nlp_stanza[lang] = stanza.Pipeline(lang, processors=processors,
                                                        tokenize_pretokenized=self.pretokenized, # pretokenized -> if we use pretokenized text
                                                        download_method=DownloadMethod.REUSE_RESOURCES,
                                                        use_gpu=False, pos_batch_size=3000)
                nlp = dict_nlp_stanza[lang]
            document = nlp(text)
            write_list, list_entities_loc = self.ner_stanza(document, user, list_entities_loc, dict_entities_tot, write_list)

        else:
            document = client.annotate(text, properties=lang_corenlp['not listed'])
            write_list, list_entities_loc = self.ner_coreNLP(document, user, write_list, list_entities_loc, org)

        print('list', list_entities_loc)


        return list_entities_loc, write_list


    def ner_tot(self, data, text_field, lang_field, user_field, lang_corenlp, lang_stanza, file_flags, output_tokens, output_documents, org):
        '''
        Detect location entities on dataset
        :param data: dataset
        :param text_field: text column
        :param lang_field: language column
        :param user_field: user column
        :param lang_corenlp: coreNLP languages
        :param lang_stanza: stanza languages
        :param file_flags: flags file (for emoticons)
        :param output_tokens: output tokens
        :param output_documents: output documents
        :param org: True/False if we also consider organizations as locations or not
        :return: total list of location entities
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("NER")

        list_tot_entities = []
        count = 0
        df_flags = pd.read_csv(file_flags, sep='\t')
        list_flags = list(df_flags['country_code'])


        with open(output_tokens, 'w') as tsvfile:

            writer = csv.writer(tsvfile, delimiter='\t') # writer tokens

            with open(output_documents, 'w') as tsvfile1:

                writer1 = csv.writer(tsvfile1, delimiter='\t') # writer documents

                writer.writerow(['user', 'token', 'entity_type'])
                writer1.writerow(['user', 'document', 'entity'])

                for i in range(len(data)):

                    el = data.at[i, text_field]
                    lang = data.at[i, lang_field]
                    user = data.at[i, user_field]

                    if not(pd.isna(el)):
                        el_trans = cleaning_ner_loc(el, list_flags)
                        print(el_trans)
                        entities, write_list = self.ner(el_trans, self.client, lang, user, lang_corenlp, lang_stanza, org)
                        list_tot_entities.append(entities)
                        for to_write in write_list:
                            writer.writerow(to_write)
                        writer1.writerow([user, el, entities])

                    writer.writerow([None, None, None])
                    pbar.update(1)
                    count += 1

                pbar.close()

        return list_tot_entities