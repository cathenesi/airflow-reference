import requests
from datetime import datetime
from currency import Currency


url = "http://data.fixer.io/api/latest?access_key={}&symbols=USD,BRL,EUR&format=1"
headers = {"Content-type": "application/json"}


def get_data(api_key, currency: Currency):
    response = requests.get(url.format(api_key), headers=headers)
    if response.status_code == 200:
        json_response = response.json()
        print(json_response)
        result_date = json_response["date"]
        usd_eur = float(json_response["rates"]["USD"])
        eur = float(json_response["rates"]["BRL"])
        usd = eur / usd_eur

        if Currency.EUR == currency:
            result_quote = eur
        elif Currency.USD == currency:
            result_quote = usd

    else:
        print('Error: ', response.text)

    return result_date, result_quote


def write_to_file(currency_filename, date, currency: Currency, currency_quote: float):
    # from airflow var CURRENCY_FILENAME
    filename = currency_filename.format(currency)
    file = open(filename, "w")
    file.write("{} | {} | {} \n".format(datetime.now(), date, currency_quote))
    print("writing file", filename, date, currency_quote)
    file.close()


def get_usd(currency_filename, currency_apikey):
    date, currency_quote = get_data(currency_apikey, Currency.USD)
    write_to_file(currency_filename, date, Currency.USD, currency_quote)


def get_eur(currency_filename, currency_apikey):
    date, currency_quote = get_data(currency_apikey, Currency.EUR)
    write_to_file(currency_filename, date, Currency.EUR, currency_quote)


if __name__ == '__main__':
    get_usd("test_file_usd", "2ab66baeac1f3a999488b301c91f9c00")
    get_eur("test_file_eur", "2ab66baeac1f3a999488b301c91f9c00")
