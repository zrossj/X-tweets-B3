from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime



default_args = {
    'owner': "airflow",
    'depends_on_past' : False,
    'retries': 0,
    'start_date':datetime(2025, 11, 5)
}


with DAG (
    default_args = default_args,
    dag_id = "A_market_sentiment-services",
    description = "Starts two services that must be run at demand - producer and postgres_injestion",
    schedule = '59 11,17,23 * * 1-5', # UTC-0
    catchup = False,
    tags = ['KAFKA', 'tweet', 'stocks']
) as dag:

    task1 = BashOperator(
        task_id = 'producer',
        bash_command = 'python /opt/airflow/dags/scripts/producer.py',
        dag = dag   
    )
    task2 = BashOperator(
        task_id = 'postgres_ingestion',
        bash_command='python /opt/airflow/dags/scripts/postgres_ingestion.py',
        dag = dag
    )

    task1 >> task2

