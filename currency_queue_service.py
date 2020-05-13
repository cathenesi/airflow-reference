from datetime import datetime
from currency import Currency
import pika


def read_from_file(currency_filename, currency: Currency):
    # from airflow var CURRENCY_FILENAME
    filename = currency_filename.format(currency)
    file = open(filename, "r")
    line = file.read().strip()
    data = line.split("|")
    return data[1], float(data[2])


def queue(currency_queue_host, date, currency: Currency, currency_quote):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=currency_queue_host))
    channel = connection.channel()
    channel.queue_declare(queue="home.app.currency", durable=True)
    body = "{}'date': {}, 'currency': {}, 'currency_quote': {}{}".format("{", date, str(currency), currency_quote, "}")
    print(body)
    channel.basic_publish(exchange="", routing_key="home.app.currency", body=body)
    connection.close()


def queue_usd(currency_filename, currency_queue_host):
    date, currency_quote = read_from_file(currency_filename, Currency.USD)
    queue(currency_queue_host, date, Currency.USD, currency_quote)


def queue_eur(currency_filename, currency_queue_host):
    date, currency_quote = read_from_file(currency_filename, Currency.EUR)
    queue(currency_queue_host, date, Currency.EUR, currency_quote)


if __name__ == '__main__':
    queue_usd("test_file_usd", "192.168.0.15")
    queue_eur("test_file_eur", "192.168.0.15")
