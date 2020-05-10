# -*- coding: utf-8 -*-

"""
"""

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


def pull_data():
    print("Pulling data")


# [START default_args]
default_args = {
    'owner': 'cathenesi',
    'start_date': datetime(2020, 5, 10, 00, 00, 00),
    'concurrency': 1,
    'email': ['cathenesi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'currency',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    description='Pull data about currency quote'
) as dag:
    task_1 = BashOperator(task_id='start_log', bash_command='echo "Start pulling currency"')
    task_2 = PythonOperator(task_id='pull_data', python_callable=pull_data)
    task_3 = BashOperator(task_id='end_log', bash_command='echo "Start pulling currency"')

# [START documentation]
dag.doc_md = __doc__

t1.doc_md = """\
#### Currency DAG
It gets data about currency on internet
"""
# [END documentation]

task_1 >> [task_2, task_3]
