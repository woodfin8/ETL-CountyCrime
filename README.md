# ETL-CountyCrime
ETL Project: Extracting data from US Census and FBI API's, Cleaning and Loading to PostgreSQL

## Purpose
Collect census and crime data for New Jersey counties for an analysis on economic factors and crime rates. 

## Code 
The entire code for the ETL project was written in Python and can be found on the Jupyter Notebook file woodfin8/ETL-CountyCrime/codes/FBI.ipynb

The following dependencies are used: 
1. requests
2. json
3. pandas
4. sqlalchemy
5. warnings

A config.py file is used to store api_keys and passwords. This is stored in the local repo and is referenced in the .gitignore file. 
api_keys and passwords are stored under the following variables:
api_key = "your_census_api_key"
fbi_key = "your_fbi_crime_api_key"
pword = "your postreSQL password"

## Extraction
County level economic and demographic data for NJ is extracted from the US Census Data API. 
A full list of Census datasets is available at https://api.census.gov/data.html
Census api keys can be requested at https://api.census.gov/data/key_signup.html

Reported crimes are extracted from the FBI's Crime Data API at  https://crime-data-explorer.fr.cloud.gov/api
FBI api keys can be requested at https://api.data.gov/signup/

The requests and json dependencies are used to grab the API json objects which are then read into pandas dataframes
The FBI "offense" data is also paginated which required a nested for loop to run through each page for each offense. Please note this request will take about 15 minutes as it loops through 300 pages of API data. 

## Transformation
Census data only required some small clean-up and re-naming of the header row. str.rstrip() is used to clean-up the county names.

The FBI data needs more clean-up work as it returns nested dictionaries of varying levels.
The "de-nest" function was written to transform the json data on reporting NJ agencies (local police departments, state police etc.) into a dataframe. 
The "un-pack" function was written to un-pack the triple nested json data for reported crimes by agency. 
The agency and crime data frames are merged and then put in a pivot table to get the county crime counts for each offense. 

## Loading
The two resulting dataframes from the Census and FBI API's are loaded to a PostgreSQL database using sqlalchemy's create_engine.
A sample SQL JOIN is run at the end of the Jupyter Notebook

