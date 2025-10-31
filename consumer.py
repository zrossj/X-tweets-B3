from confluent_kafka import Consumer
from dotenv import load_dotenv
import logging
import os
import json
from s3_bucket import aws_s3


load_dotenv()


conf = {
    'bootstrap.servers': os.getenv('bootstrap_servers'),
    'group.id': os.getenv('group_id'),
    'auto.offset.reset': 'earliest'}


consumer = Consumer(conf)
consumer.subscribe(
    [os.getenv('kafka_topic')]
)

s3 = aws_s3()

# code logic for consumer to deal with kafka streaming - also, s3 call for save raw tweets;
try:
    while True:
        msg = consumer.poll(timeout = 1)    
        
        if msg is None:
            continue
            
        if msg.error():
            logging.info(f'Error.')
        
        logging.info(f"Received message: {msg.value().decode('utf-8')}")

        if msg is not None and not msg.error():
            print(f"{msg.value().decode('utf-8')}")
            msg_asdict = json.loads(msg.value().decode('utf-8'))

            for key, value in msg_asdict.items():
                response = s3.put_in_s3(key, json.dumps(value))
                logging.info(
                    "Msg sent to S3 bucket for storage\n",
                    response,
                    '\n'
                )
                

except KeyboardInterrupt:
    logging.info(f'Interrupted by the user.')

finally:
    logging.info('Finalizing')
