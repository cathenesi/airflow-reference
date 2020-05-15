# Exemplo de DAG com airflow

A DAG conecta-se a uma API e baixa valores do Euro e Dólar; os valores são salvos em arquivos e, depois, publicados em uma fila.

- currency_pipeline.py: declaração da DAG e tasks
- currency_api_service.py: serviço de conexão à API
- currency_queue_service: serviço de conexão à fila de mensagens

O projeto não tem o arquivo requirements.txt:
- as dependências da DAG são resolvidas no Airflow
- para conexão à fila, é necessária a biblioteca pika (pip install pika)
