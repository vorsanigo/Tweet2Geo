# Tweet2Geo

The aim of the project is to extract two types of locations from tweets: *user's main location* and *locations from 
text*, extracted respectively from the fields `location` and `text`.
Once done it, we try to create networks where there are connections created considering the main location of the user 
and the mentioned places inside the  text.

Here it is possible to find some scripts to perform tweets collection and analysis, focused on geolocation.


## Scripts description

- `preprocessing.py`, to do preprocessing before performing NER or using Nominatim
- `nominatim_ner_geocoding.py`, here there are 5 main functions:
  - `find_tot_loc_nominatim_ner_pipeline` (save into csv) and `find_tot_loc_nominatim_ner_pipeline_tsv` (save into tsv) 
  to map the text inside the `location` field to a location using *Nominatim* and *NER* (more precise but slower)
  - `find_tot_loc_nominatim_pipeline` (save into csv) and `find_tot_loc_nominatim_pipeline_tsv` (save into tsv)
  to map the text inside the `location` field to a location using only *Nominatim* (less precise but faster)
  - `find_tot_loc_text_nominatim` to map locations extracted from the text using *Nominatim*
- `main_geocoding.py`, to easily use the two functions (Nominatim+NER to tsv / only Nominatim to tsv) to map locations
- `ner.py`, to perform NER using *CoreNLP* / *Stanza*
- `twitter_api.py`, to create queries for the twitter API (NB: set now to perform queries to randomly collect tweets in 
a certain time range and from a certain country)
- `tweets_dataset.py`, to create a dataset of tweets with their fields saved in different columns
- `tweets_flux.py`, to extract locations and map them using *Nominatim* by calling a function from 
`nominatim_ner_geocoding.py`


## Requirements

Some files/tools are required to run some scripts. In particular, all the scripts that use `ner.py` need 
*CoreNLP*/*Stanza* models, while to perform preprocessing it can be required to have the two files `unicode_flags.csv` 
and `weird_words.csv`, which can be found here in the repository