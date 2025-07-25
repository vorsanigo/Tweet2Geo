import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from umap import UMAP
from matplotlib import pyplot as plt
import argparse
import torch
torch.cuda.empty_cache()


# arguments parser
parser = argparse.ArgumentParser(description='Embeddings parameters')
parser.add_argument('-region',
                    type=str,
                    default='EU',
                    help='Region of the world: EU, SA, US')
parser.add_argument('-documents_to_clean',
                    type=str,
                    default='/cleaned_text_loc_no_text_EU 1.csv',
                    help='path of document to clean:\
                        Europe: /cleaned_text_loc_no_text_EU 1.csv\
                        South America: /cleaned_text_loc_no_text_inside_SA.csv\
                        United States: /cleaned_text_no_text_loc_inside_US 111.csv')
parser.add_argument('-hdbscan_param',
                    type=int,
                    default=70,
                    help='HDBSCAN parameter')
parser.add_argument('-model',
                    type=str,
                    default='sentence-transformers/paraphrase-multilingual-mpnet-base-v2',
                    help='Model to use for embeddings')
args = parser.parse_args()


# region of the world
region = args.region # EU, SA, US

# dataset
file = region + args.documents_to_clean


# model name
model_name =  args.model 


# output
dir_output = region + '/'
embeddings_df = dir_output + 'embeddings_info_no_extra_state_' + region + '.csv'
embeddings_np = dir_output + 'embeddings_no_extra_state_' + region + '.npy'
reduced_embeddings_np = dir_output + 'reduced_embeddings_no_extra_state_' + region + '.npy'
cleaned_df = dir_output + 'completely_cleaned_no_extra_state_' + region + '.csv'

# columns to use
cleaned_text_col = 'cleaned_text'
tweet_id_col = 'id'


### EMBEDDINGS

# Load a pretrained Sentence Transformer model
model = SentenceTransformer(model_name) 

# dataset
df = pd.read_csv(file)
print('len df tot', len(df))


# EU specifics to remove tweets in vatican city and vatican work from the text
# NB: uncomment it only if needed
'''if args.region == 'EU':
    # remove tweets in vatican city
    df = df[df['place_country'] != 'VA'].reset_index()
    # remove rows with tweets text = nan
    df = df[~df[cleaned_text_col].isna()]
    # remove vatican word
    df['cleaned_text'] = df['cleaned_text'].apply(lambda x: x.replace('vatican', ''))
    df = df.reset_index()'''

# Us specifics
# remove tweets geotagged/mentioning alaska, hawaii, washington, guam, puerto rico
if args.region == 'US':
    df = df[~df['state_y'].isin(['AK', 'HI', 'WA'])]
    df = df[~df['state_x'].isin(['Alaska', 'Hawaii', 'Washington', 'Guam', 'Puerto Rico'])]


# using tweets text
# remove rows with tweets text = nan
df = df[~df[cleaned_text_col].isna()]
print('len df no nan', len(df))
# remove duplicated text
df = df.drop_duplicates(subset=[cleaned_text_col])
print('len df no duplicates', len(df))
# reset index
df = df.reset_index()
# save completely cleaned df
df.to_csv(cleaned_df, index=False)


# tweet id
tweet_ids = df[tweet_id_col].tolist()

# The sentences to encode -> cleaned text column
sentences = df[cleaned_text_col].tolist()

# Calculate embeddings by calling model.encode()
embeddings = model.encode(sentences)
print('Computed embeddings')

# reduce embeddings for visualization purposes
umap_model = UMAP(n_neighbors=15, n_components=2, min_dist=0.0, metric='cosine', random_state=42)
reduced_embeddings = UMAP(n_neighbors=15, n_components=2, min_dist=0.0, metric='cosine', random_state=42).fit_transform(embeddings)
print('Computed reduced embeddings')

# df embeddings
df_embeddings = pd.DataFrame(columns=['tweet id', 'embeddings', 'reduced embeddings'])
df_embeddings['tweet id'] = tweet_ids
df_embeddings['embeddings'] = list(np.array(embeddings))
df_embeddings['reduced embeddings'] = list(np.array(reduced_embeddings))
df_embeddings.to_csv(embeddings_df, index=False)

# save embeddings
np.save(embeddings_np, embeddings)
np.save(reduced_embeddings_np, reduced_embeddings)