import requests
from enum import Enum
from datetime import datetime


class Currency(Enum):
    USD = 'USD'
    EUR = 'EUR'


url = 'http://data.fixer.io/api/latest?access_key=2ab66baeac1f3a999488b301c91f9c00&symbols=USD,BRL,EUR&format=1'
headers = {'Content-type': 'application/json'}


def get_data(currency: Currency):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_response = response.json()
        print(json_response)
        result_date = json_response['date']
        usd_eur = float(json_response['rates']['USD'])
        eur = float(json_response['rates']['BRL'])
        usd = eur / usd_eur

        if Currency.EUR == currency:
            result_quote = eur
        elif Currency.USD == currency:
            result_quote = usd

    else:
        print('Error: ', response.text)

    return result_date, result_quote


def write(date, currency: Currency, currency_quote: float):
    file = open("$AIRFLOW_HOME/currency_data/{}-{}.txt".format(currency, date), "a")
    file.write("{} - {} \n".format(datetime.now(), currency_quote))
    print(date, currency_quote)
    file.close()


def get_eur():
    date, currency_quote = get_data(Currency.EUR)
    write(date, Currency.EUR, currency_quote)


def get_usd():
    date, currency_quote = get_data(Currency.USD)
    write(date, Currency.USD, currency_quote)


if __name__ == '__main__':
    get_usd()
    get_eur()
