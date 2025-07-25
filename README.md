# Tweet2Geo

This project aims to understand the international interest and attention between countries by using geolocated Twitter data from three regions of the world: Europe, South America, and the United States.
It builds the networks of international interest and it adopts gravity models to explain the patterns and drivers of the mentioning behavior between countries. Finally, it extracts the topics from the text of the tweets to contextualize the obtained results.

## Structure

### Folders

- `Data` contains the aggregated data divided into the three regions (subfolders `Europe`, `South America`, and `US`): number of mentions per day between countries, data for the gravity model, socio-economic data
- `Data collection` contains scripts for the data collection:
  - *twitter_api.py* to download data from Twitter through its API
  - *tweets_dataset.py* to save data into organizaed datasets
  - *tweets_flux.py* to compute and save number of mentions per day between countries
  - *preprocessing.py* to do preprocessing of tweets text
  - *nominatim_ner_geocoding.py* to compute geocoding using nominatim
  - *main_geocoding.py* main to run geocoding
  - *new_ner.py* to extract locations from text using NER from Stanza
- `Data analysis` contains scripts divided into five subfolders:
  - `GDP VS tweets`: file *f_001_figure_GDP_tweets.ipynb* to produce the image GDP per capita VS tweets per day per country
  - `Network measures`: files *EU_network_measures.ipynb*, *SA_network_measures.ipynb*, *US_network_measures no wa.ipynb* to compute network measures and generate networks images
  - `Choroplets`: files *f_003_tweets_flux_EU.ipynb*, *f_003_tweets_flux_SA.ipynb*, *f_003_tweets_flux_US_no_WA.ipynb* to generate choroplet maps
  - `Gravity model`: files *f_002_gravity model_EU_img.ipynb*, *f_002_gravity model_SA_img.ipynb*, *f_002_gravity model_US_img.ipynb* to run gravity models and get corresponding plot
  - `Topics`: file *embeddings.py* to generate embeddings, file *topics.py* to extract topics, file *topics_check.ipynb* to compute extra analysis on topics results
 
### Nice structure

```
├── Data
│   ├── Europe
│   │   ├── 0 fluxes 0.5 
│   │   ├── 0 gravity model
│   ├── South America
│   │   ├── 0 fluxes 0.5
│   │   ├── 0 gravity model
│   ├── US
│   │   ├── 0 gravity model
├── Data analysis
│   ├── Choroplets
│   │   ├── EU
│   │   ├── SA
│   │   ├── US
│   ├── GDP VS tweets
│   │   ├── f_001_figure_GDP_tweets.ipynb
│   ├── Gravity model
│   │   ├── EU
│   │   ├── SA
│   │   ├── US
│   │   ├── f_002_gravity model_EU_img.ipynb
│   │   ├── f_002_gravity model_SA_img.ipynb
│   │   └── f_002_gravity model_US_img.ipynb
│   ├── Network measures
│   │   ├── EU_network_measures.ipynb
│   │   ├── SA_network_measures.ipynb
│   │   └── US_network_measures no wa.ipynb
│   └── Topics
│       ├── EU
│       ├── SA
│       ├── US
│       ├── embeddings.py
│       ├── topics.py
├── Data collection
│   ├── main_geocoding.py
│   ├── new_ner.py
│   ├── nominatim_ner_geocoding.py
│   ├── preprocessing.py
│   ├── tweets_dataset.py
│   ├── tweets_flux.py
│   └── twitter_api.py
├── README.md
└── requirements.txt
```

 

## Installation

1) Clone the repository:
   `git clone https://github.com/vorsanigo/Tweet2Geo.git`
2) In the cloned folder, create a virtual environment via `pip` using the `requirements.txt` file:
   virtualenv venv -r requirements.txt
3) kernel -> TODO

## Execution

### Data collection and processing
1) **Twitter data collection**
   - Run script `twitter_api.py` to download Twitter data
   - Run script `tweets_dataset.py` to save data into organized datasets
2) **Count mentions between countries**
   - Run script `tweets_flux.py` to compute and save the number of mentions per day between countries
3) **Location extraction**
   - Run script `preprocessing.py` to preprocess tweets text
  
### Data analysis
1) Inside folder `GDP VS tweets`: notebook `f_001_figure_GDP_tweets.ipynb` to produce the image GDP per capita VS tweets per day per country
2) Inside folder `Network measures`: notebooks `EU_network_measures.ipynb`, `SA_network_measures.ipynb`, `US_network_measures no wa.ipynb` to compute network measures and generate networks images respectively for Europe, South America, and the United States
3) Inside folder `Choroplets`: notebooks `f_003_tweets_flux_EU.ipynb`, `f_002_gravity model_SA_img.ipynb`, `f_003_tweets_flux_US_no_WA.ipynb` to generate choroplet maps respectively for Europe, South America, and the United States
4) Inside folder `Gravity model`: notebooks `f_002_gravity model_EU_img.ipynb`, `f_002_gravity model_SA_img.ipynb`, `f_002_gravity model_US_img.ipynb` to run gravity models and get corresponding plots respectively for Europe, South America, and the United States
5)  Inside folder `Topics`:
     - run script `embeddings.py` to extract the embeddings from the tweets text (you can change the arguments to pass by command line: region of the world (EU, SA, US), dataset for embeddings extraction, model for embeddings extraction)
     -  run script `topics.py` to extract the topics from the tweets (you can change the arguments to pass by command line: region of the world (EU, SA, US), UMAP and HDBSCAN parameters, model used for embeddings extraction)
    
