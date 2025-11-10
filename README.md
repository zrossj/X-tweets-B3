HOW IT WORKS

Tweets can be obtained by queries and an access TOKEN provided by X. In this project, the queries searches for tweets of some brazilian stocks (like Petrobras [#PETR4]), Brent Oil and Gold.
The tweets are queried and transmited to Kafka by the producer (producer.py) script. 
The tweets messages are streamed to a consumer (consumer.py) where the data is sent to AWS S3 for storage (raw).
The postgres_ingestion.py stores the tweets in a table format on Postgres which can be queried and used for BI or ML.  
The DAG will executes the producer in 6 hrs window, starting at 12 (UTC), 18 and 00:00 and after it runs, the postgres ingestion also run. 
The KAFKA, Postgres and the Python Consumer runs as docker containers. The ingestion and producer are tasks from a Airflow's DAG which is also containerized.  



HOW TO RUN

1. You need Docker installed. https://docs.docker.com/get-started/
2. Configure the .env_ file on the parental folder (same where README.md is located. Renamed it to .env after setting it up). The variables for postgres connection, AWS S3 and the KAFKA SERVER must be set. The .env_ is a scratch for which variables you need to define. The bucket_name and bucket_prefix are examples. You can change those. Do not change the bootstrap_server or the postgres_port (unless you know what you are doing).
3. Create 2 empty folders named 'logs' and 'plugins'. They will be used by Airflow.
4. Build the custom image for Apache Airflow (./$ docker build -t extended_airflow ./app/custom_airflow)
5. Run Docker compose up
6. Thats it. You can check on localhost:8080 in a web-browser to see the DAG tasks and runs in the Airflow UI, logs etc. 



