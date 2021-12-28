import datetime
import pytest
import requests_mock

import market_data_loader.clients.marketstack_client as client

SUCCESS_RESPONSE = """
{
    "pagination": {
        "limit": 2,
        "offset": 0,
        "count": 2,
        "total": 2
    },
    "data": [
        {
            "close": 10.1,
            "symbol": "AAPL",
            "exchange": "XNAS",
            "date": "2021-04-09T00:00:00+0000"
        },
        {
            "close": 20.2,
            "symbol": "AAPL",
            "exchange": "XNAS",
            "date": "2021-04-10T00:00:00+0000"
        }
    ]
}
"""

MULTI_PAGE_SUCCESS_RESPONSES = [
    """
    {
        "pagination": {
            "limit": 2,
            "offset": 0,
            "count": 2,
            "total": 4
        },
        "data": [
            {
                "close": 10.1,
                "symbol": "AAPL",
                "exchange": "XNAS",
                "date": "2021-04-09T00:00:00+0000"
            },
            {
                "close": 20.2,
                "symbol": "AAPL",
                "exchange": "XNAS",
                "date": "2021-04-10T00:00:00+0000"
            }
        ]
    }
    """,
    """
    {
        "pagination": {
            "limit": 2,
            "offset": 2,
            "count": 2,
            "total": 4
        },
        "data": [
            {
                "close": 30.3,
                "symbol": "AAPL",
                "exchange": "XNAS",
                "date": "2021-04-11T00:00:00+0000"
            },
            {
                "close": 40.4,
                "symbol": "AAPL",
                "exchange": "XNAS",
                "date": "2021-04-12T00:00:00+0000"
            }
        ]
    }
    """,
]

ERROR_RESPONSE = """
{
   "error": {
      "code": "validation_error",
      "message": "Request failed with validation error",
      "context": {
         "symbols": [
            {
               "key": "missing_symbols",
               "message": "You did not specify any symbols."
            }
         ]
      }
   }
}
"""


def test_success(requests_mock):
    symbol = "AAPL"
    start_date = datetime.date(2021, 4, 9)
    end_date = datetime.date(2021, 4, 10)

    request_params = "&".join(
        [
            "access_key=00000000000000000000000000000000",
            "symbols=AAPL",
            "sort=ASC",
            "date_from=2021-04-09",
            "date_to=2021-04-10",
            "limit=1000",
            "offset=0",
        ]
    )

    request_url = f"http://api.marketstack.com/v1/eod?{request_params}"

    requests_mock.get(request_url, text=SUCCESS_RESPONSE)

    response = client.end_of_day(symbol, start_date, end_date)
    response_page = response.__next__()

    assert response_page[0]["symbol"] == symbol
    assert response_page[0]["close"] == 10.1
    assert response_page[0]["date"] == "2021-04-09T00:00:00+0000"
    assert response_page[1]["symbol"] == symbol
    assert response_page[1]["close"] == 20.2
    assert response_page[1]["date"] == "2021-04-10T00:00:00+0000"

    with pytest.raises(StopIteration):
        response.__next__()


def test_multi_page_success(requests_mock):
    symbol = "AAPL"
    start_date = datetime.date(2021, 4, 9)
    end_date = datetime.date(2021, 4, 12)

    request_1_params = "&".join(
        [
            "access_key=00000000000000000000000000000000",
            "symbols=AAPL",
            "sort=ASC",
            "date_from=2021-04-09",
            "date_to=2021-04-12",
            "limit=1000",
            "offset=0",
        ]
    )

    requests_mock.get(
        f"http://api.marketstack.com/v1/eod?{request_1_params}",
        text=MULTI_PAGE_SUCCESS_RESPONSES[0],
    )

    request_2_params = "&".join(
        [
            "access_key=00000000000000000000000000000000",
            "symbols=AAPL",
            "sort=ASC",
            "date_from=2021-04-09",
            "date_to=2021-04-12",
            "limit=2",
            "offset=2",
        ]
    )

    requests_mock.get(
        f"http://api.marketstack.com/v1/eod?{request_2_params}",
        text=MULTI_PAGE_SUCCESS_RESPONSES[1],
    )

    response = client.end_of_day(symbol, start_date, end_date)
    response_page_1 = response.__next__()

    assert response_page_1[0]["symbol"] == symbol
    assert response_page_1[0]["close"] == 10.1
    assert response_page_1[0]["date"] == "2021-04-09T00:00:00+0000"
    assert response_page_1[1]["symbol"] == symbol
    assert response_page_1[1]["close"] == 20.2
    assert response_page_1[1]["date"] == "2021-04-10T00:00:00+0000"

    response_page_2 = response.__next__()

    assert response_page_2[0]["symbol"] == symbol
    assert response_page_2[0]["close"] == 30.3
    assert response_page_2[0]["date"] == "2021-04-11T00:00:00+0000"
    assert response_page_2[1]["symbol"] == symbol
    assert response_page_2[1]["close"] == 40.4
    assert response_page_2[1]["date"] == "2021-04-12T00:00:00+0000"

    with pytest.raises(StopIteration):
        response.__next__()


def test_failure(requests_mock):
    symbol = "AAPL"
    start_date = datetime.date(2021, 4, 9)
    end_date = datetime.date(2021, 4, 10)

    requests_mock.get(
        "http://api.marketstack.com/v1/eod", text=ERROR_RESPONSE, status_code=422
    )

    response = client.end_of_day(symbol, start_date, end_date)

    with pytest.raises(RuntimeError) as error:
        response.__next__()

    assert (
        error.value.args[0]
        == "Error while fetching stock prices: Request failed with validation error"
    )
