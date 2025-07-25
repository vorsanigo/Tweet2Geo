import pandas as pd
#from sentence_transformers import SentenceTransformer
#from umap import UMAP
from matplotlib import pyplot as plt
import numpy as np
#import torch
import ast
import argparse
import pandas as pd
import pickle
from nltk.corpus import stopwords
import nltk
from bertopic import BERTopic
from huggingface_hub import login
from sklearn.feature_extraction.text import CountVectorizer
import gensim.corpora as corpora
from gensim.models.coherencemodel import CoherenceModel
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from torch import cuda
from torch import bfloat16
import transformers
from umap import UMAP
from hdbscan import HDBSCAN
from sentence_transformers import SentenceTransformer
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, TextGeneration

import torch
torch.cuda.empty_cache()

import config_topics
#from dotenv import load_dotenv
#load_dotenv()

nltk.download('stopwords')


# INITIALIZATION -> parameters to set

#https://sbert.net/
# API KEY
HUGGINGFACE_TOKEN = config_topics.HUGGINGFACE_TOKEN
login(token=HUGGINGFACE_TOKEN)


# arguments parser
parser = argparse.ArgumentParser(description='Topic modeling parameters')
parser.add_argument('-region',
                    type=str,
                    default='EU',
                    help='Region of the world: EU, SA, US')
parser.add_argument('-umap_param',
                    type=int,
                    default=15,
                    help='UMAP parameter')
parser.add_argument('-hdbscan_param',
                    type=int,
                    default=70,
                    help='HDBSCAN parameter')
parser.add_argument('-model',
                    type=str,
                    default='sentence-transformers/paraphrase-multilingual-mpnet-base-v2',
                    help='Model used for embeddings')
args = parser.parse_args()


# NB set umap and hdbscan values
# NB create df for topics and documents accordingly to type of data

# model embeddings
model = args.model # distilbert-multilingual-nli-stsb-quora-ranking

# region of the world
region = args.region # EU, SA, US

# embeddings folder
emb_dir = region + '/'

# inputs
file_embeddings_info = emb_dir + 'embeddings_info_' + region + '.csv'
file_embeddings = emb_dir + 'embeddings_' + region + '.npy'
file_documents = emb_dir + '/completely_cleaned_' + region + '.csv'

# documents column
documents_column = 'cleaned_text'
# tweet id column
tweet_id_col = 'id'

# umap and hdbscan parameters
umap_param = args.umap_param # 15
hdbscan_param = args.hdbscan_param #30 #70 #150

# outputs
df_topic_document = emb_dir + 'topic_' + region + '_' + str(hdbscan_param) + '.csv'
df_topic_info = emb_dir + 'doc_info_' + region + '_' + str(hdbscan_param) + '.csv'
topic_model_path = emb_dir + 'topic_model_' + str(hdbscan_param) + '_' + region



# LOAD EMBEDDINGS, DOCUMENTS AND DEFINE OUTPUT DF

# df embeddings
#print('df embeddings')
df_embeddings = pd.read_csv(file_embeddings_info)
#print('len embeddings', len(df_embeddings))
#print('Columns of embeddings df', df_embeddings.columns)
#print('Length embeddings:', len(df_embeddings))
# df tweets
#print('df tweets')
df_documents = pd.read_csv(file_documents)
df_documents = df_documents[~df_documents[documents_column].isna()]
#print('Columns of documents df', df_documents.columns)
#print('Length documents:', len(df_documents))
# embedings
embeddings_0 = np.load(file_embeddings)
embeddings = embeddings_0

print('Embeddings loaded')


# set variables
# list of documents
#print(df_documents.columns)
documents_list = df_documents[documents_column].tolist()
# create df eith documents and related topic number
# set columns names
topics_df_new = pd.DataFrame()
topics_df_new['tweet id'] = df_documents[tweet_id_col]
topics_df_new[documents_column] = df_documents[documents_column]


# serialization settings for saving the topic model
'''serialization_pickle = 'pickle'
serialization_'''


# remove stopwords
if args.region == 'EU':
    lang_stopwords = ['english', 'spanish', 'german', 'italian', 'spanish', 'portuguese', 'french', 'arabic']
elif args.region == 'SA':
    lang_stopwords = ['spanish', 'english']
elif args.region == 'US':
    lang_stopwords = ['english', 'spanish']
stopwords = stopwords.words(lang_stopwords)


# use CountVectorizer to remove stopwords
vectorizer_model = CountVectorizer(stop_words=stopwords)


# set quantization configuration to load large model with less GPU memory
# this requires the `bitsandbytes` library

'''bnb_config = transformers.BitsAndBytesConfig(
    load_in_4bit=True,  # 4-bit quantization
    bnb_4bit_quant_type='nf4',  # Normalized float 4
    bnb_4bit_use_double_quant=True,  # Second quantization after the first
    bnb_4bit_compute_dtype=bfloat16  # Computation type
)'''


# UMAP AND HDBSCAN

# set submodels
# umap
umap_model = UMAP(n_neighbors=umap_param, n_components=5, min_dist=0.0, metric='cosine', random_state=42)
# hdbscan
hdbscan_model = HDBSCAN(min_cluster_size=hdbscan_param, metric='euclidean', cluster_selection_method='eom', prediction_data=True) # min_cluster_size=30, 70


# BERTopic

embedding_model = SentenceTransformer(model)

# representation model
# KeyBERT
keybert = KeyBERTInspired()
# MMR
mmr = MaximalMarginalRelevance(diversity=0.3)
# Text generation with Llama 2
#llama2 = TextGeneration(generator, prompt=prompt)

# All representation models
representation_model = {
    "KeyBERT": keybert,
    #"Llama2": llama2,
    "MMR": mmr,
}

topic_model = BERTopic(

  # Sub-models
  embedding_model=embedding_model,
  vectorizer_model=vectorizer_model,
  umap_model=umap_model,
  hdbscan_model=hdbscan_model,
  representation_model=representation_model,

  # Hyperparameters
  top_n_words=10,
  verbose=True,
  #nr_topics = n

  calculate_probabilities=True
  
)

# Train model
topics, probs = topic_model.fit_transform(documents_list, embeddings)

print('Topics computed')

# add topics to df
topics_df_new['topic_num'] = topics

# Count outliers
outliers_count = topics.count(-1)
# Reduce outliers
if outliers_count > 0:
    # trivial strategy
    new_topics = topic_model.reduce_outliers(documents_list, topics)
    # probabilities
    topics_prob = topic_model.reduce_outliers(documents_list, topics, probabilities=probs, strategy="probabilities")
    # topic distributions
    topics_distr = topic_model.reduce_outliers(documents_list, topics, strategy="distributions")
    # c-TF-IDF
    topics_c_tf_idf = topic_model.reduce_outliers(documents_list, topics, strategy="c-tf-idf")
    # embeddings
    topics_embeddings = topic_model.reduce_outliers(documents_list, topics, strategy="embeddings")
    
    # add topics to df
    topics_df_new['topic_num_forced'] = new_topics
    topics_df_new['topic_num_prob'] = topics_prob
    topics_df_new['topic_num_distr'] = topics_distr
    topics_df_new['topic_num_c_tf_idf'] = topics_c_tf_idf
    topics_df_new['topic_num_embeddings'] = topics_embeddings

print('Outliers reduced')


#Hierarchical topics
hierarchical_topics = topic_model.hierarchical_topics(documents_list)
topic_model.visualize_hierarchy(hierarchical_topics=hierarchical_topics)
tree = topic_model.get_topic_tree(hierarchical_topics)
print(tree)

print('Hierarchical topics computed')

print(topic_model.get_topic_info())
print(topic_model.topic_representations_)
print(topic_model.vectorizer_model)
print(topic_model.embedding_model)


# SAVE STUFF

# save topics df
topics_df_new.to_csv(df_topic_document, index=False)

# get topics info
info_df = topic_model.get_topic_info()
info_df.to_csv(df_topic_info, index=False)

# save topic model
topic_model.save(topic_model_path + '_pickle', serialization='pickle')
topic_model.save(topic_model_path + 'pytorch', serialization='pytorch', save_ctfidf=True, save_embedding_model=model)
topic_model.save(topic_model_path + 'safetensor', serialization='safetensors', save_ctfidf=True, save_embedding_model=model)

print('Topic modeling saved')