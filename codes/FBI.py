#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
import json
import pandas as pd
import numpy as np
from config import api_key, fbi_key,pword
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')


# In[5]:


#write api call for 2017 data and get response
long_url2 = "https://api.census.gov/data/2017/acs/acs5/profile?get=DP05_0001E,DP02_0037PE,DP02_0061PE,DP03_0009PE,DP03_0128PE,DP03_0063E,DP05_0004E,DP05_0018E,NAME&for=county:*&in=state:34&key="
response2 = requests.get(long_url2 + api_key)
print(response2)


# In[ ]:


#read json
response2=response2.json()


# In[ ]:


#convert to dataframe and clean up header
census_pd = pd.DataFrame(response2)
census_pd.columns = census_pd.iloc[0]
census_pd = census_pd.iloc[1:]
census_pd.head()


# In[ ]:


#rename columns
census_pd = census_pd.rename(columns={"DP05_0001E": "population","DP02_0037PE": "pct_births_single_moms",
                                                  "DP02_0061PE": "pct_hs_diploma","DP03_0009PE":"unemployment",
                                                  "DP03_0128PE":"pct_under_pvt_lvl","DP03_0063E":"mean_income",
                                                  "DP05_0004E":"sex_ratio","DP05_0018E":"mean_age", "NAME":"county_name"})
census_pd.head()


# In[ ]:


census_pd.keys()


# In[ ]:


#re-order columns
census_pd= census_pd[['county','state','county_name','population', 'pct_births_single_moms', 'pct_hs_diploma',
       'unemployment', 'pct_under_pvt_lvl', 'mean_income', 'sex_ratio',
       'mean_age']]


# In[ ]:


census_pd.head()


# In[ ]:


#clean up county name strings
census_pd['county_name'] = census_pd['county_name'].str.rstrip('New Jersey')
census_pd['county_name'] = census_pd['county_name'].str.rstrip('County,')

census_pd.head()


# In[ ]:


#write df to csv
census_pd.to_csv("../output_files/census.csv")


# In[ ]:


#write api call NJ police departments from FBI API and get response
long_url2 = "https://api.usa.gov/crime/fbi/sapi/api/agencies/byStateAbbr/raw/NJ?API_KEY="
response2 = requests.get(long_url2+fbi_key)
print(response2)


# In[ ]:


#read json
response2=response2.json()


# In[ ]:


#define function to "de-nest" the multi-layered dictionary

def de_nest(nest):
    dict1 = {}
    
    for key in nest.values():
        dict1.update(key)
    
    list1 = []
    for key in dict1.values():
        list1.append(key)
    
    df1 = pd.DataFrame(list1)
    
    return df1
    
    


# In[ ]:


#create nj police dept dataframe
nj_df = de_nest(response2)


# In[ ]:


#check dataframe
nj_df.head()


# In[ ]:


#get keys
nj_df.keys()


# In[ ]:


#drop unwanted columns
nj_df.drop(['agency_type_name', 'division_name','latitude', 'longitude','nibrs','nibrs_start_date','region_desc',
            'region_name', 'state_abbr', 'state_name'], inplace=True, axis=1)
nj_df.head()


# In[ ]:


#export to csv
nj_df.to_csv("../output_files/agencies.csv")


# In[ ]:


#create list of offenses in the FBI API
offenses_list = ["aggravated-assault","burglary","larceny","motor-vehicle-theft",
                 "homicide","rape","robbery","arson","violent-crime","property-crime"]


# In[ ]:


#run for loop to run through each offence and every API page per offence 
offense_jsons = []
url1 = "https://api.usa.gov/crime/fbi/sapi/api/summarized/state/NJ/"
url2 = "/2017/2017?API_KEY="
for crime in offenses_list:
    url = url1 + crime + url2 + fbi_key
    response = requests.get(url).json()
    num_pages = response["pagination"]["pages"]
    for page in range(num_pages + 1):
        response2 = requests.get(url1+crime+url2+fbi_key, params={'page': page})
        print(response2)
        response2 = response2.json()
        offense_jsons.append(response2)


# In[ ]:


#delete pagination key
for x in range(len(offense_jsons)):
    del offense_jsons[x]['pagination']


# In[ ]:


#create inpack function to unpack nested dictionaries and convert to dataframe
def unpack(jumble):
    [list1] = [key for key in jumble.values()]
    df1 = pd.DataFrame(list1)
    return df1
    
    


# In[ ]:


# Use pd.concat to merge a list of DataFrame into a single big DataFrame.
new_df = []
for x in range(len(offense_jsons)):
    data = unpack(offense_jsons[x])
    new_df.append(data)
    
new_df = pd.concat(new_df)
# Use pd.concat to merge a list of DataFrame into a single big DataFrame.


# In[ ]:


#reset index and check tail
new_df.reset_index(drop=True).tail()


# In[ ]:


#merge crime and agency dataframes
crime_df = pd.merge(new_df, nj_df, on="ori",how="left")


# In[ ]:


#check table length
print(f'Our crime table has {crime_df.shape[0]} rows')
crime_df.head()


# In[ ]:


crime_df.to_csv("../output_files/FBI.csv")


# In[ ]:


#create pivot table summing total offenses by type by county
crime_pivot = pd.pivot_table(crime_df, index= 'county_name', columns= 'offense', values= "actual", fill_value=0, aggfunc='sum')
crime_pivot


# In[ ]:


#flatten the table
crime_pivot.columns = crime_pivot.columns.get_level_values('offense')
crime_pivot.reset_index(inplace=True)


# In[ ]:


#check crime pivot
crime_pivot


# In[ ]:


#assign county id's to crime pivot table
id_df = census_pd[['county','state','county_name']]
id_df['county_name'] = id_df['county_name'].str.upper() 
id_df.sort_values('county_name', inplace=True)
id_df.reset_index(drop=True, inplace=True)
id_df.head()


# In[ ]:


#strip out any white space before merging on county_name
id_df["county_name"] = id_df["county_name"].str.strip()
crime_pivot["county_name"] = crime_pivot["county_name"].str.strip()


# In[ ]:


#merge county id's with crime data
county_crime = pd.merge(crime_pivot, id_df, on="county_name")
county_crime


# In[ ]:


#reorder columns and delete columns with no data
county_crime = county_crime[[ 'state', 'county','county_name','aggravated-assault',
 'arson','burglary','homicide','larceny','motor-vehicle-theft','property-crime','rape','robbery','violent-crime'
]]


# In[ ]:


#create column that sums all offenses for each county
county_crime['total_offenses']= county_crime.iloc[:, 3:12].sum(axis=1)


# In[ ]:


county_crime.head()


# In[ ]:


county_crime.to_csv("../output_files/county_crime.csv")


# In[6]:


#connect to local database
rds_connection_string = "postgres:"+ pword + "@localhost:5432/nj_crime"
engine = create_engine(f'postgresql://{rds_connection_string}')


# In[ ]:


#export census data to SQL
census_pd.to_sql(name='census', con=engine, if_exists='replace', index=True)


# In[ ]:


#export crime data to SQL 
county_crime.to_sql(name='crime', con=engine, if_exists='replace', index=True)


# In[ ]:


engine.table_names()


# In[ ]:


#read tables from SQL
pd.read_sql("SELECT * FROM census",con=engine)


# In[ ]:


pd.read_sql("SELECT * FROM crime",con=engine)


# In[ ]:


#run a test JOIN 
pd.read_sql_query('SELECT cr.county_name, cr.total_offenses, ce.Pct_Under_Pvt_Lvl FROM crime AS cr JOIN census AS ce ON cr.county = ce.county;', con=engine)


# In[ ]:




