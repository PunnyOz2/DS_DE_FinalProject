from datetime import datetime, timedelta
import json
import redis
import os
import urllib.request
import zipfile

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Pun',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}


def read_data_to_redis():
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    print('flushing db')
    r.flushdb()
    for filename in os.listdir('data'):
        f = os.path.join('data', filename)
        print('doing', f)
        if os.path.isfile(f):
            with open(f) as file:
                data = json.load(file)
                count = 0
                Maxcount = len(data)
                for item in data:
                    r.set(f"paper:{item['eid']}:references",
                          json.dumps(item['references']))
                    r.sadd("papereids", item['eid'])
                    r.set(f"paper:{item['eid']}:affiliations",
                          json.dumps(item['affiliations']))
                    count += 1
                    print(f"{count}/{Maxcount} done")

# r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
# print(r.keys())
# print(r.smembers("papereids"))
# temp = r.srandmember("papereids")
# print(temp)
# print(r.get(f"paper:{temp}:references")[0])
# print(json.loads(r.get(f"paper:{temp}:references"))[0])


def print_random_paper():
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    temp = r.srandmember("papereids")
    print(temp)
    print(r.get(f"paper:{temp}:references")[0])
    print(json.loads(r.get(f"paper:{temp}:references"))[0])


def download_data():
    url = 'https://github.com/PunnyOz2/data_for_DE_project/raw/main/archive.zip'
    urllib.request.urlretrieve(url, 'archive.zip')
    with zipfile.ZipFile('archive.zip', 'r') as zip_ref:
        zip_ref.extractall('data')


with DAG(
    dag_id='paper_data_dag_v2',
    default_args=default_args,
    description='A DAG to read data to redis',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # schedule_interval='@daily',
) as dag:
    task0 = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    task1 = PythonOperator(
        task_id='read_data_to_redis',
        python_callable=read_data_to_redis,
    )

    task2 = PythonOperator(
        task_id='print_random_paper',
        python_callable=print_random_paper,
    )

    task0 >> task1 >> task2
