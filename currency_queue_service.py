from datetime import datetime
from currency import Currency


def read_from_file(currency_filename, currency: Currency):
    # from airflow var CURRENCY_FILENAME
    filename = currency_filename.format(currency)
    file = open(filename, "r")
    line = file.read().strip()
    data = line.split("|")
    return data[1], float(data[2])


def queue_usd(currency_filename):
    date, currency_quote = read_from_file(currency_filename, Currency.USD)
    print(date, currency_quote)


def queue_eur(currency_filename):
    date, currency_quote = read_from_file(currency_filename, Currency.EUR)
    print(date, currency_quote)


if __name__ == '__main__':
    queue_usd('test_file_usd')
    queue_eur('test_file_eur')
