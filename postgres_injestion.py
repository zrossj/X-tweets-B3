#%%
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import boto3
import os
import re
import json
from s3_bucket import aws_s3

load_dotenv()
bucket_name = os.getenv('bucket_name')

# SETUP AWS CONNECTION
s3 = aws_s3()._connect_s3_()
prefix = 'raw_data/'

response = s3.list_objects_v2(
    Bucket = bucket_name, Prefix = prefix)      # holds all the 'files' stored in raw_data/ on AWS;

#%%
objects_data = []
objects_key = []

for obj in response['Contents']:
    key = obj['Key']
    obj_response = s3.get_object(
        Bucket = bucket_name,
        Key = key
    )
    objects_data.append(obj_response)
    objects_key.append(key)

files_content = []

for file in objects_data:
    content_raw = file['Body'].read()
    content_utf = content_raw.decode('utf-8')
    content_json = json.loads(content_utf)
    files_content.append(content_json)

# EACH FILE IS A COLLECTION OF TWEETS, SO WE NEED TO JSON.LOAD IT ASWELL;

tweets = []
for tweets_pack in files_content:
    tweets.append(json.loads(tweets_pack))


# CLEANING KEYS TO GET DATE-TIME PART ONLY
obj_keys_clean = [re.search('\d+-\d+', key).group() if re.search('\d+-\d+', key) else 'error' for key in objects_key]

recon = {}
for key, vals in zip(obj_keys_clean, tweets):
    recon[key] = vals


# CREATING A DATAFRAME

df = pd.DataFrame(recon) \
    .melt(ignore_index = False) \
    .dropna() \
    .reset_index()

df.columns = ['tweet_id', 'datetime', 'body']


# INSERTING ON POSTGRESQL
psql_user = os.getenv('postgres_user')
db_name = os.getenv('postgres_db')
db_port = os.getenv('postgres_port')
password = os.getenv('postgres_password')

uri = f"postgresql+psycopg2://{psql_user}:{password}@localhost:{db_port}/{db_name}"
engine = create_engine(uri)


with engine.connect() as conn:

    df.to_sql(
        name = 'tweets_ibovstocks', con = conn, 
        if_exists= 'replace', index = False)

