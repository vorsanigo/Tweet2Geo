'''
Class to perform NER on text
'''

import pandas as pd
import logging
from tqdm import tqdm
import stanza
stanza.install_corenlp()
from stanza.server import CoreNLPClient
from stanza.pipeline.core import DownloadMethod
import csv
from preprocessing import *

#stanza.download_corenlp_models(model='italian', version='4.2.2', dir="~/stanza_corenlp")
# todo NB: PROBLEM -> IF YOU LOAD THE STNAZA MODELS ALL TOGETHER -> MEMORY ISSUES


class NERLocation():

    def __init__(self, pretokenized = False,
                        client = CoreNLPClient(
                                annotators=['tokenize', 'ner'],
                                memory='4G',
                                endpoint='http://localhost:8000',
                                be_quiet=True)
                 ):
        self.client = client
        self.pretokenized = pretokenized    


    def ner_tot(self, data, text_field, lang_field, user_field, dict_nlp_stanza, output_documents):
        '''
        Detect location entities on dataset
        :param data: dataset
        :param text_field: text column
        :param lang_field: language column
        :param user_field: user column
        :param dict_nlp_stanza: dictionary of stanza models
        :param output_documents: output documents
        :return: total list of location entities
        '''

        logger = logging.StreamHandler()
        logger.setLevel(logging.ERROR)
        pbar = tqdm(total=len(data), position=1)
        pbar.set_description("NER")

        count = 0

        processors = 'tokenize, ner'
        dict_nlp_stanza = {}


        with open(output_documents, 'w') as tsvfile:

            writer = csv.writer(tsvfile, delimiter='\t') # writer documents

            writer.writerow(['user', 'document', 'entity', 'lang'])

            for i in range(len(data)):

                text = data.at[i, text_field] 
                lang = data.at[i, lang_field]
                user = data.at[i, user_field]

                if not(pd.isna(text)):

                    el = clean_tweet_ner_loc(text)
                    list_entities_loc = []
                    if lang in dict_nlp_stanza:
                        nlp = dict_nlp_stanza[lang]
                    else:
                        dict_nlp_stanza[lang] = stanza.Pipeline(lang, processors=processors,
                                                            tokenize_pretokenized=self.pretokenized, # pretokenized -> if we use pretokenized text
                                                            download_method=DownloadMethod.REUSE_RESOURCES,
                                                                use_gpu=False, pos_batch_size=3000)
                    nlp = dict_nlp_stanza[lang]
                    document = nlp(el)
                    for sentence in document.sentences:
                        if sentence.ents != []:
                            for entity in sentence.ents:
                                if entity.type == 'LOC':
                                    list_entities_loc.append(entity.text)
                            for token in sentence.tokens:
                                if token.text.lower() in list_entities_loc:
                                    writer.writerow([user, el, token.text, lang])

                pbar.update(1)
                count += 1

        pbar.close()  


        return dict_nlp_stanza