
import pandas as pd
from bertopic import BERTopic
import os
from numpy import savetxt
from datetime import datetime
from umap import UMAP
#from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
import stopwordsiso
import torch
import argparse
import json
from pathlib import Path
from tqdm import tqdm
from random import sample, seed
from transformers import Trainer, TrainingArguments, set_seed, BitsAndBytesConfig
from transformers import AutoModelForCausalLM, AutoTokenizer
import re
from glob import glob
import sys
import config_llm

#italian_stopwords.extend(["super", "wien", "pause", "germany", "austria", "mega", "fein", "pre", "obwohl", "beste", "franzen", "lugar", "österreich", "imgur", "buffet", "sofia"])


HUGGINGFACE_TOKEN = config_llm.LLM_TOKEN
dir_data = 'Data analysis/Topics/topics_results/'

language = 'English'#
# call llama
#nltk.download('punkt')
#italian_stopwords = list(stopwordsiso.stopwords(lang))


gen_model = "mistralai/Mixtral-8x7B-Instruct-v0.1"
gen_model = "CohereLabs/aya-expanse-32b"
current_path = os.getcwd()
print(current_path)

device = torch.cuda.current_device()

device_map = {"": 0}


bnb_config = BitsAndBytesConfig(load_in_4bit=True,
                                bnb_4bit_use_double_quant=True,
                                bnb_4bit_quant_type="nf4",
                                bnb_4bit_compute_dtype=torch.bfloat16
                                )



tokenizer = AutoTokenizer.from_pretrained(gen_model, use_auth_token=HUGGINGFACE_TOKEN)
model = AutoModelForCausalLM.from_pretrained(gen_model,
                                                use_auth_token=HUGGINGFACE_TOKEN,
                                                device_map=device_map,
                                                quantization_config=bnb_config,
                                                cache_dir="./cache", #cache_dir="/raid/home/dh/.cache/huggingface/hub/",
                                            #  load_in_4bit=True,
                                                )


myfiles = os.listdir(dir_data)

for file in myfiles:
    if 'label' not in file:
        try:

            df = pd.read_csv(dir_data+file.replace('.tsv','')+ '_label.tsv',sep =',')
            print(file+' already done')
        except:
            print(file)
            df = pd.read_csv(dir_data + file, sep=',')
            for ind, row in df.iterrows():
                words = row["Representation"]
                #print(words)
                #prompt_t = f"The following sentence is the about section of a Telegram channel. Try to assign a label (2, 3 or 4 words max) that can give an idea of what is the topic about. Only return one potential name.\nWords: {words}\nName:"
                prompt_t = f"The following words are from the description about the content of a cluster of tweet posts (I did topic modelling with Bert topic and clustering on a dataset of tweets). Please summarise this with a descriptive label in {language} (4 or 5 words max). Only return one potential name.\nWords: {words}\nName:"
                print(str(ind))
                prompt = tokenizer(prompt_t, return_tensors="pt")["input_ids"]
                # prompt = prompt.to(device)
                prompt = prompt.to('cuda')
                generated = model.generate(inputs=prompt,
                                                eos_token_id=tokenizer.eos_token_id,
                                                pad_token_id=tokenizer.eos_token_id,
                                                # output_scores=True,
                                                return_dict_in_generate=True,
                                                do_sample=True,
                                                top_p=0.95,
                                                    # "top_p": 0.95,
                                                top_k= 50,
                                                repetition_penalty= 1.2,
                                                num_return_sequences=1,
                                                max_new_tokens=8,
                                                min_length=1,
                                                # bad_words_ids=bad_words_ids,
                                                # no_repeat_ngram_size=4,
                                                )
                
                sequences = generated["sequences"]
                decoded_seqs = tokenizer.batch_decode(sequences, skip_special_tokens=True)
                decoded_seq = decoded_seqs[0].replace(prompt_t, "")
                new_name = decoded_seq
                #print(row["Name"])
                #print(new_name)
                
                df.loc[ind, 'about-label_prompt1'] = new_name    
                prompt_t = f"The following words are from the description about the content of a cluster of tweet posts (I did topic modelling with Bert topic and clustering on a dataset of tweets). Please summarise this with a descriptive label in {language}. Only return one potential name.\nWords: {words}\nName:"
                print(str(ind))
                prompt = tokenizer(prompt_t, return_tensors="pt")["input_ids"]
                # prompt = prompt.to(device)
                prompt = prompt.to('cuda')
                generated = model.generate(inputs=prompt,
                                                eos_token_id=tokenizer.eos_token_id,
                                                pad_token_id=tokenizer.eos_token_id,
                                                # output_scores=True,
                                                return_dict_in_generate=True,
                                                do_sample=True,
                                                top_p=0.95,
                                                    # "top_p": 0.95,
                                                top_k= 50,
                                                repetition_penalty= 1.2,
                                                num_return_sequences=1,
                                                max_new_tokens=8,
                                                min_length=1,
                                                # bad_words_ids=bad_words_ids,
                                                # no_repeat_ngram_size=4,
                                                )
                
                sequences = generated["sequences"]
                decoded_seqs = tokenizer.batch_decode(sequences, skip_special_tokens=True)
                decoded_seq = decoded_seqs[0].replace(prompt_t, "")
                new_name = decoded_seq
                #print(row["Name"])
                #print(new_name)
                
                df.loc[ind, 'about-label_prompt2'] = new_name    

                df.to_csv(dir_data + file.replace('.tsv','')+ '_label.tsv',sep = '\t', index = False)
