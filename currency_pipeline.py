# -*- coding: utf-8 -*-
"""
Exemplo de DAG que conecta a serviço REST para obter dados de cotação de moedas
"""

from datetime import timedelta, datetime
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from currency_api_service import get_eur, get_usd
from currency_queue_service import queue_usd, queue_eur

# [START argumentos da DAG]
default_args = {
    "owner": "cathenesi",
    "start_date": datetime(2020, 5, 10, 00, 00, 00),
    "concurrency": 1,
    "email": ['cathenesi@gmail.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=2)
}
# [END argumentos]

# [START instanciação da DAG]
with DAG(
        "currency",
        catchup=False,
        default_args=default_args,
        schedule_interval="*/30 * * * *",
        description="Pull data about currency quote"
) as dag:
    # dollar
    task_1_1 = BashOperator(task_id="start_usd", bash_command="echo '>>> Log: start collecting dollar data'")
    task_1_2 = PythonOperator(task_id="get_USD_data",
                              op_kwargs={"currency_filename": "{{var.value.currency_filename}}",
                                         "currency_apikey": "{{var.value.currency_apikey}}"},
                              python_callable=get_usd)
    task_1_3 = PythonOperator(task_id="queue_USD_data",
                              op_kwargs={"currency_filename": "{{var.value.currency_filename}}",
                                         "currency_queue_host": "{{var.value.currency_queue_host}}"},
                              python_callable=queue_usd)
    # euro
    task_2_1 = BashOperator(task_id="start_eur", bash_command="echo '>>> Log: start collecting euro data'")
    task_2_2 = PythonOperator(task_id="get_EUR_data",
                              op_kwargs={"currency_filename": "{{var.value.currency_filename}}",
                                         "currency_apikey": "{{var.value.currency_apikey}}"},
                              python_callable=get_eur)
    task_2_3 = PythonOperator(task_id="queue_EUR_data",
                              op_kwargs={"currency_filename": "{{var.value.currency_filename}}",
                                         "currency_queue_host": "{{var.value.currency_queue_host}}"},
                              python_callable=queue_eur)

    # end
    task_3 = BashOperator(task_id="end", bash_command="echo '>>> Log: Ending pulling currency'")
# [END instanciação]

# [START documentação]
dag.doc_md = __doc__

task_1_1.doc_md = """\
#### Log the begin of the process for US Dollar
Just a bash log
"""

task_1_2.doc_md = """\
#### Get US Dollar data
Connect to the REST Service
"""

task_1_3.doc_md = """\
#### Send US Dollar data to queue
Rabbit MQ
"""

task_2_1.doc_md = """\
#### Log the begin of the process for Euro
Just a bash log
"""

task_2_2.doc_md = """\
#### Get Euro data
Connect to the REST Service
"""

task_2_3.doc_md = """\
#### Send Euro data to queue
Rabbit MQ
"""

task_3.doc_md = """\
#### Log the end of the process
Just a bash log
"""
# [END documentação]

# [Ordem das tasks]
task_1_1 >> task_1_2 >> task_1_3 >> task_3 << task_2_3 << task_2_2 << task_2_1
