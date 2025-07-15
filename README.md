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
  - `GDP VS tweets`: file *f_001_figure_GDP_tweets.ipynb* to produce the image gdp per capita VS tweets per day per country
  - `Network measures`: files *EU_network_measures.ipynb*, *SA_network_measures.ipynb*, *US_network_measures no wa.ipynb* to compute network measures and generate networks images
  - `Choroplets`: files *f_003_tweets_flux_EU.ipynb*, *f_003_tweets_flux_SA.ipynb*, *f_003_tweets_flux_US_no_WA.ipynb* to generate choroplet maps
  - `Gravity model`: files *f_002_gravity model_EU_img.ipynb*, *f_002_gravity model_SA_img.ipynb*, *f_002_gravity model_US_img.ipynb* to run gravity models and get corresponding plot
  - `Topics`: file *embeddings.py* to generate embeddings, file *topics.py* to extract topics, file *topics_check.ipynb* to compute extra analysis on topics results
 

## Execution


