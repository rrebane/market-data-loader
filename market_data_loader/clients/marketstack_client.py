import datetime
import logging
import os
import requests
import sys

# The free subscription does not support HTTPS
API_BASE_URL = "http://api.marketstack.com/v1"
FETCH_LIMIT = 1000


def end_of_day(symbol, start_date, end_date):
    logging.info(
        "Fetching stock prices from MarketStack for '%s' from '%s' to '%s'",
        symbol,
        start_date,
        end_date,
    )

    def request_fn(limit, offset):
        params = {
            "access_key": _access_key(),
            "symbols": [symbol],
            "sort": "ASC",
            "date_from": start_date.strftime("%Y-%m-%d"),
            "date_to": end_date.strftime("%Y-%m-%d"),
            "limit": limit,
            "offset": offset,
        }

        response = requests.get(f"{API_BASE_URL}/eod", params=params)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)

        return response.json()

    return _paginate(request_fn)


def _paginate(request_fn, limit=FETCH_LIMIT):
    offset = 0
    count = 0
    total = sys.maxsize

    while (offset + count) < total:
        response = request_fn(limit, offset + count)
        yield response["data"]

        pagination = response["pagination"]
        limit = pagination["limit"]
        offset = pagination["offset"]
        count = pagination["count"]
        total = pagination["total"]


def _access_key():
    if not "MARKET_STACK_ACCESS_KEY" in os.environ:
        raise RuntimeError("MARKET_STACK_ACCESS_KEY environment variable missing")

    return os.environ["MARKET_STACK_ACCESS_KEY"]


def _handle_http_error(err):
    error_message = None

    try:
        error_message = err.response.json()["error"]["message"]
    except:
        raise RuntimeError("Error while fetching stock prices") from err

    raise RuntimeError(f"Error while fetching stock prices: {error_message}") from err
