import logging
import os

import requests

# The free subscription does not support HTTPS
API_BASE_URL = "http://api.exchangeratesapi.io/v1"
# The free subscription only supports EUR as base currency
BASE_CURRENCY = "EUR"


def currencies():
    logging.info("Fetching currency rate symbols")

    params = {"access_key": _access_key()}
    response = requests.get(f"{API_BASE_URL}/symbols", params=params)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        _handle_http_error("Error while fetching currency symbols", err)

    return response.json()


def currency_rate(date, currencies):
    logging.info(
        "Fetching currency rates from ExchangeRatesAPI at '%s' from '%s' to %s",
        date,
        BASE_CURRENCY,
        currencies,
    )

    params = {
        "access_key": _access_key(),
        "base": BASE_CURRENCY,
        "symbols": ",".join(currencies),
    }

    date_str = date.strftime("%Y-%m-%d")
    response = requests.get(f"{API_BASE_URL}/{date_str}", params=params)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        _handle_http_error("Error while fetching currency rates", err)

    return response.json()


def _access_key():
    if not "EXCHANGE_RATES_API_ACCESS_KEY" in os.environ:
        raise RuntimeError("EXCHANGE_RATES_API_ACCESS_KEY environment variable missing")

    return os.environ["EXCHANGE_RATES_API_ACCESS_KEY"]


def _handle_http_error(prefix, err):
    error_message = None

    try:
        error_message = err.response.json()["error"]["message"]
    except:
        raise RuntimeError(prefix) from err

    raise RuntimeError(f"{prefix}: {error_message}") from err
