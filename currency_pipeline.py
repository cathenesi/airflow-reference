# -*- coding: utf-8 -*-
"""
Example of process that reads currency quote data and publishes it into a queue
"""

from datetime import timedelta, datetime
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

url = 'http://data.fixer.io/api/latest?access_key=2ab66baeac1f3a999488b301c91f9c00&symbols=USD,BRL,EUR&format=1'
headers = {'Content-type': 'application/json'}


def pull_data():
    print("Pulling data")
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        print(response.text)
    else:
        print('Error: ', response.text)


# [START default_args]
default_args = {
    'owner': 'cathenesi',
    'start_date': datetime(2020, 5, 10, 00, 00, 00),
    'concurrency': 1,
    'email': ['cathenesi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'currency',
    catchup=False,
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    description='Pull data about currency quote'
) as dag:
    task_1 = BashOperator(task_id='start_log', bash_command='echo ">>> Log: start pulling currency"')
    task_2 = PythonOperator(task_id='pull_USD_data', python_callable=pull_data)
    task_3 = PythonOperator(task_id='pull_EUR_data', python_callable=pull_data)
    task_4 = BashOperator(task_id='end_log', bash_command='echo ">>> Log: Ending pulling currency"')

# [START documentation]
dag.doc_md = __doc__

task_1.doc_md = """\
#### Log the begin of the process
Just log
"""

task_2.doc_md = """\
#### Pull US Dollar data
Connect to the REST Service
"""

task_3.doc_md = """\
#### Pull Euro data
Connect to the REST Service
"""

task_4.doc_md = """\
#### Log the end of the process
Just log
"""
# [END documentation]

task_1 >> task_2 >> task_3 >> task_4
