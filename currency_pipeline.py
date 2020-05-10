# -*- coding: utf-8 -*-
"""
Exemplo de DAG que conecta a serviço REST para obter dados de cotação de moedas
"""

from datetime import timedelta, datetime
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from currency_service import get_eur, get_usd


# [START argumentos da DAG]
default_args = {
    'owner': 'cathenesi',
    'start_date': datetime(2020, 5, 10, 00, 00, 00),
    'concurrency': 1,
    'email': ['cathenesi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}
# [END argumentos]

# [START instanciação da DAG]
with DAG(
    'currency',
    catchup=False,
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    description='Pull data about currency quote'
) as dag:
    task_1 = BashOperator(task_id='start_log', bash_command='echo ">>> Log: start pulling currency"')
    task_2 = PythonOperator(task_id='get_USD_data', python_callable=get_usd)
    task_3 = PythonOperator(task_id='get_EUR_data', python_callable=get_eur)
    task_4 = BashOperator(task_id='end_log', bash_command='echo ">>> Log: Ending pulling currency"')
# [END instanciação]

# [START documentação]
dag.doc_md = __doc__

task_1.doc_md = """\
#### Log the begin of the process
Just log
"""

task_2.doc_md = """\
#### Get US Dollar data
Connect to the REST Service
"""

task_3.doc_md = """\
#### Get Euro data
Connect to the REST Service
"""

task_4.doc_md = """\
#### Log the end of the process
Just log
"""
# [END documentação]

# [Ordem das tasks]
task_1 >> task_2 >> task_3 >> task_4
