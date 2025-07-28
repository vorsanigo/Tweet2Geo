# Tweet2Geo

This project aims to understand the international interest and attention between countries by using geolocated Twitter data from three regions of the world: Europe, South America, and the United States.
It builds the networks of international interest and it adopts gravity models to explain the patterns and drivers of the mentioning behavior between countries. Finally, it extracts the topics from the text of the tweets to contextualize the obtained results.

## Structure

```
├── Data                                        <- Aggregated data for each region: mentions                                              
│   ├── Europe                                     per day between countries, gravity model data, 
│   ├── South America                              socio-economic data
│   ├── US
├── Data analysis                               <- Scripts for the data analysis
│   ├── Choroplets                              <- Notebooks to generate choroplet maps
│   │   ├── f_003_tweets_flux_EU.ipynb
│   │   ├── f_003_tweets_flux_SA.ipynb
│   │   ├── f_003_tweets_flux_US_no_WA.ipynb
│   ├── GDP VS tweets                           <- Notebook to generate GDP per capita VS tweets per day per country
│   │   ├── f_001_figure_GDP_tweets.ipynb                
│   ├── Gravity model                           <- Notebooks to run gravity models and get corresponding plots
│   │   ├── f_002_gravity model_EU_img.ipynb    
│   │   ├── f_002_gravity model_SA_img.ipynb
│   │   └── f_002_gravity model_US_img.ipynb
│   ├── Network measures                        <- Notebooks to compute network measures and generate network images
│   │   ├── EU_network_measures.ipynb
│   │   ├── SA_network_measures.ipynb
│   │   └── US_network_measures no wa.ipynb
│   └── Topics                                  <- Scripts for embeddings generation and topics modeling
│       ├── embeddings.py                       
│       ├── topics.py                           
├── Data collection                             <- Scripts for the data collection and processing
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
2) In the cloned folder, create a virtual environment through the command `virtualenv venv` and activate it through `source venv/bin/activate`
3) Inside the virtual environment, install the requirements through `pip install -r requirements.txt`
4) kernel -> TODO

## Execution

### Data collection and processing

The Twitter API conditions changed so the data collection as performed by us is no longer available. However, we report here the strategy we followed.
1) **Twitter data collection**
   - Script `twitter_api.py` to download Twitter data
   - Script `tweets_dataset.py` to save data into organized datasets
2) **Location extraction and geocoding**
   - Script `preprocessing.py` to preprocess tweets text
   - Script `new_ner.py` to extract locations from text using Stanza
   - Script `main_geocoding.py` to geocode location strings given in input in a dataframe (to assign geographic information to the locations found in the tweets text)
3) **Count mentions between countries**
   - Script `tweets_flux.py` to compute and save the number of mentions per day between countries
     
The final datasets containing the matrices of the mentions per day between countries are the following:
- Inside `Data/Europe/`: file `fluxes_0.5_norm_day_ok.csv`
- Inside `Data/South America/`: file `fluxes_0.5_norm_day okok.csv`
- Inside `Data/US/`: file `fluxes_0.5_norm_day no wa.csv`
  
### Data analysis
1) Inside folder `GDP VS tweets`: notebook `f_001_figure_GDP_tweets.ipynb` to produce the image GDP per capita VS tweets per day per country
2) Inside folder `Network measures`: notebooks `EU_network_measures.ipynb`, `SA_network_measures.ipynb`, `US_network_measures no wa.ipynb` to compute network measures and generate networks images respectively for Europe, South America, and the United States
3) Inside folder `Choroplets`: notebooks `f_003_tweets_flux_EU.ipynb`, `f_002_gravity model_SA_img.ipynb`, `f_003_tweets_flux_US_no_WA.ipynb` to generate choroplet maps respectively for Europe, South America, and the United States
4) Inside folder `Gravity model`: notebooks `f_002_gravity model_EU_img.ipynb`, `f_002_gravity model_SA_img.ipynb`, `f_002_gravity model_US_img.ipynb` to run gravity models and get corresponding plots respectively for Europe, South America, and the United States
5)  Inside folder `Topics`:
     - run script `embeddings.py` to extract the embeddings from the tweets text (you can change the arguments to pass by command line: region of the world (EU, SA, US), dataset for embeddings extraction, model for embeddings extraction)
     -  run script `topics.py` to extract the topics from the tweets (you can change the arguments to pass by command line: region of the world (EU, SA, US), UMAP and HDBSCAN parameters, model used for embeddings extraction)
    
