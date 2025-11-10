#%%
import tweepy
from dotenv import load_dotenv
import os
import datetime as dt
from confluent_kafka import Producer
import json
import logging

load_dotenv()

logging.basicConfig(
    level = logging.INFO,
    handlers= [logging.StreamHandler()],
    format = "%(asctime)s [%(levelname)s] (%(filename)s) - %(message)s"
)

## LOAD VARIABLES .ENV
token = os.getenv("TWITTER_BEARER_TOKEN")
kafka_topic = os.getenv('kafka_topic')
bootstrap_server = os.getenv('bootstrap_servers')

# config KAFKA
conf = {
    'bootstrap.servers': bootstrap_server
}


# SET TWEEPY'S CLIENT
try:
    client = tweepy.Client(bearer_token=token)
except Exception as e:
    logging.info(f"Problem connecting to X API: {e}")

# DEFINE QUERY FOR FILTERING DATA FROM TWEETS 
stocks_list = ["BBAS3", "PETR3" , "PETR4",  "CMIN3",  "VAMO3",  "BBSE3", "LEVE3", "RECV3"]
stocks_list_with_hash = ['#'+stock for stock in stocks_list]
query_stocks_part = " OR ".join(stocks_list + stocks_list_with_hash)
query_other = ' OR #IRONORE OR #BRENT OR "oil prices"'
query = f'({query_stocks_part}{query_other}) -is:retweet lang:pt'
assert len(query) < 512, 'Query is too long for the limited free acess.'

#%%
# CALLING X's API
start_time = (dt.datetime.now() - dt.timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")

try:
    query_result = client.search_recent_tweets(
        query = query, start_time = start_time, tweet_fields=['text'], max_results = 100
    )
except Exception as e:
    logging.info(f'Error at querying X: {e}')

# JSON TO BE SENT
request = {}
for tweet in query_result.data:
    request[tweet.id] = tweet.text

now = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
message = {now: request}

# KAFKA PRODUCER
producer = Producer(conf)

if query_result:
    producer.produce(
        topic = kafka_topic,
        value = json.dumps(message).encode('utf-8')
    )

    producer.flush()
    logging.info('Message sent to Kafka')

else:
    logging.info("No data sent to Kafka")
