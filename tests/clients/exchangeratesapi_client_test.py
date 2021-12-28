import datetime
import pytest
import requests_mock

import market_data_loader.clients.exchangeratesapi_client as client

CURRENCIES_SUCCESS_RESPONSE = """
{
  "success": true,
  "symbols": {
    "AED": "United Arab Emirates Dirham",
    "AFN": "Afghan Afghani",
    "ALL": "Albanian Lek",
    "AMD": "Armenian Dram"
    }
}
"""

CURRENCIES_ERROR_ERROR_RESPONSE = """
{
  "error": {
    "code": "invalid_access_key",
    "message": "You have not supplied a valid API Access Key. [Technical Support: support@apilayer.com]"
  }
}
"""

CURRENCY_RATE_SUCCESS_RESPONE = """
{
    "success": true,
    "historical": true,
    "date": "2013-12-24",
    "timestamp": 1387929599,
    "base": "GBP",
    "rates": {
        "USD": 1.636492,
        "EUR": 1.196476,
        "CAD": 1.739516
    }
}
"""

CURRENCY_RATE_ERROR_RESPONSE = """
{
  "error": {
    "code": "invalid_currency_codes",
    "message": "You have provided one or more invalid Currency Codes. [Required format: currencies=EUR,USD,GBP,...]"
  }
}
"""


def test_currencies_success(requests_mock):
    request_url = "http://api.exchangeratesapi.io/v1/symbols?access_key=00000000000000000000000000000000"

    requests_mock.get(request_url, text=CURRENCIES_SUCCESS_RESPONSE)

    response = client.currencies()

    assert response["symbols"]["AED"] == "United Arab Emirates Dirham"
    assert response["symbols"]["AFN"] == "Afghan Afghani"
    assert response["symbols"]["ALL"] == "Albanian Lek"
    assert response["symbols"]["AMD"] == "Armenian Dram"


def test_currencies_failure(requests_mock):
    requests_mock.get(
        "http://api.exchangeratesapi.io/v1/symbols",
        text=CURRENCIES_ERROR_ERROR_RESPONSE,
        status_code=401,
    )

    with pytest.raises(RuntimeError) as error:
        client.currencies()

    assert (
        error.value.args[0]
        == "Error while fetching currency symbols: You have not supplied a valid API Access Key. [Technical Support: support@apilayer.com]"
    )


def test_currency_rate_success(requests_mock):
    date = datetime.date(2013, 12, 24)
    currencies = ["USD", "EUR", "CAD"]

    symbols = ",".join(currencies)

    request_params = "&".join(
        [
            "access_key=00000000000000000000000000000000",
            "base=EUR",
            f"symbols={symbols}",
        ]
    )

    request_url = f"http://api.exchangeratesapi.io/v1/2013-12-24?{request_params}"

    requests_mock.get(request_url, text=CURRENCY_RATE_SUCCESS_RESPONE)

    response = client.currency_rate(date, currencies)

    assert response["date"] == "2013-12-24"
    assert response["rates"]["USD"] == 1.636492
    assert response["rates"]["EUR"] == 1.196476
    assert response["rates"]["CAD"] == 1.739516


def test_currency_rate_failure(requests_mock):
    date = datetime.date(2013, 12, 24)
    currencies = ["USD", "EUR", "CAD"]

    requests_mock.get(
        "http://api.exchangeratesapi.io/v1/2013-12-24",
        text=CURRENCY_RATE_ERROR_RESPONSE,
        status_code=422,
    )

    with pytest.raises(RuntimeError) as error:
        client.currency_rate(date, currencies)

    assert (
        error.value.args[0]
        == "Error while fetching currency rates: You have provided one or more invalid Currency Codes. [Required format: currencies=EUR,USD,GBP,...]"
    )
