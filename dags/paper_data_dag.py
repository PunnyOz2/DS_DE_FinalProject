from datetime import datetime, timedelta
import json
import redis
import os
import urllib.request
import zipfile

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from given_data_function import cleaningProcess
from scraped_data_function import clean_caller, scrapData
default_args = {
    'owner': 'Pun',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}


def read_data_to_redis():
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    print('flushing db')
    r.flushdb()
    for filename in os.listdir('/opt/cleaned_data'):
        f = os.path.join('/opt/cleaned_data', filename)
        print('doing', f)
        if os.path.isfile(f) and f.endswith('.json'):
            with open(f) as file:
                data = json.load(file)
                count = 0
                Maxcount = len(data)
                for item in data:
                    for reference in item['references']:
                        r.rpush(
                            f"paper:{item['eid']}:references", json.dumps(reference))
                    r.sadd("papereids", item['eid'])
                    r.sadd(f"papereids:{filename[-9:-5]}", item['eid'])
                    for affiliation in item['affiliations']:
                        if ('country' in affiliation and str(affiliation['country']).lower() == 'thailand'):
                            continue
                        r.rpush(
                            f"paper:{item['eid']}:affiliations", json.dumps(affiliation))
                    count += 1
                    print(f"{count}/{Maxcount} done")


def print_random_paper():
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    temp = r.srandmember("papereids")
    print(temp)
    print(r.lrange(f"paper:{temp}:references", 0, -1))
    print(r.lrange(f"paper:{temp}:affiliations", 0, -1))


def download_scraped_data():
    # url = 'https://github.com/mrmatchax/DataScienceProject/raw/master/raw_scopus.zip'
    # urllib.request.urlretrieve(url, '/opt/raw_data/raw_scraped_data.zip')
    # with zipfile.ZipFile('/opt/raw_data/raw_scraped_data.zip', 'r') as zip_ref:
    #     zip_ref.extractall('/opt/raw_data/raw_scraped_data')
    pass

with DAG(
    dag_id='clean_data_to_redis_dag',
    default_args=default_args,
    description='A DAG to clean paper data and read to redis',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # schedule_interval='@daily',
) as dag:
    
    read_data_to_redis = PythonOperator(
        task_id='read_data_to_redis',
        python_callable=read_data_to_redis,
    )

    given_data_cleaning = PythonOperator(
        task_id='given_data_cleaning',
        python_callable=cleaningProcess,
        op_args=['/opt/raw_data/raw_given_data', '/opt/cleaned_data'],
    )

    scraped_data_cleaning = PythonOperator(
        task_id='scraped_data_cleaning',
        python_callable=clean_caller,
    )

    [given_data_cleaning, scraped_data_cleaning] >> read_data_to_redis

with DAG(
    dag_id='download_scraped_data_dag',
    default_args=default_args,
    description='A DAG to download raw paper data',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # schedule_interval='@daily',
) as dag:

    trigger_clean_data_to_redis_dag = TriggerDagRunOperator(
        task_id='trigger_clean_data_to_redis_dag',
        trigger_dag_id='clean_data_to_redis_dag',
    )

    download_scraped_data = PythonOperator(
        task_id='download_scraped_data',
        python_callable=scrapData,
    )

    download_scraped_data >> trigger_clean_data_to_redis_dag