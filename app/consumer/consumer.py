#%%
from confluent_kafka import Consumer
from dotenv import load_dotenv
import logging
import os
import json
from s3_bucket import aws_s3


load_dotenv()

logging.basicConfig(
    level = logging.INFO,
    handlers = [logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] (%(filename)s) - %(message)s"
)

kafka_topic = os.getenv('kafka_topic')
bootstrap_server = os.getenv('bootstrap_servers')
kafka_group_id = os.getenv('group_id')


conf = {
    'bootstrap.servers': bootstrap_server,
    'group.id': kafka_group_id,
    'auto.offset.reset': 'earliest'}


consumer = Consumer(conf)
consumer.subscribe(
    [kafka_topic]
)

s3 = aws_s3()

#%%
# CODE LOGIC FOR CONSUMER TO DEAL WITH KAFKA STREAMING - ALSO, S3 CALL FOR SAVE RAW TWEETS;
try:
    while True:
        msg = consumer.poll(timeout = 1)    
        
        if msg is None:     #no msg on stream
            continue
            
        if msg.error():
            logging.error(f'Msg error: {msg.error()}')
            continue
        
        # IF IS A VALID MESSAGE:
        msg_decoded = msg.value().decode('utf-8')
        logging.info(f"Received message: {msg_decoded}")
        try:
            msg_asdict = json.loads(msg.value().decode('utf-8'))
            for key, value in msg_asdict.items():

                response = s3.put_in_s3(key, json.dumps(value))
                logging.info("Message sent to S3 bucket for storage {response}")

        except Exception as e:
            logging.exception(f"Error processing message: {e}")
                

except KeyboardInterrupt:
    logging.info(f'Interrupted by the user')

finally:
    logging.info('Finalizing')
