#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd 


# In[8]:


# pd.read_csv("/Users/zichuchen/Downloads/yellow_tripdata_2021-01.csv")
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = f'{prefix}yellow_tripdata_2021-01.csv.gz'

df=pd.read_csv(url)


# In[9]:


df.head()


# In[10]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# In[11]:


df = pd.read_csv(url, dtype = dtype, parse_dates = parse_dates)
df.head()


# In[12]:


get_ipython().system('uv add sqlalchemy')


# In[13]:


get_ipython().system('uv add psycopg2-binary')


# In[16]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[17]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[18]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[19]:


df.head(0)


# In[23]:


it = pd.read_csv(
    url,
    dtype = dtype,
    parse_dates = parse_dates,
    iterator=True,
    chunksize = 100000
)


# In[24]:


for df_chunk in it:
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




