# market-data-loader

A simple program to fetch stock prices for given dates and display the prices in
a specified currency. The program caches the data locally and re-uses it where
possible.

## Setup

### Set up Python environment

```bash
python3 -m virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

### Initialize local database

```bash
source env/bin/activate
./create_schema.py
```

## Test

```bash
pytest
```

## Run

```bash
usage: load_data.py [-h] --symbol SYMBOL --currency CURRENCY [--start-date START_DATE] [--end-date END_DATE] [--verbose | --no-verbose]

Fetch and display stock prices in specified currency

optional arguments:
  -h, --help            show this help message and exit
  --symbol SYMBOL       stock symbol ('AAPL')
  --currency CURRENCY   currency symbol ('USD')
  --start-date START_DATE
                        start date ('YYYY-mm-dd', default: today's date)
  --end-date END_DATE   end date ('YYYY-mm-dd', default: today's date)
  --verbose, --no-verbose
                        verbose logging (default: False)
```

The program expects the access keys for https://marketstack.com/ and
https://exchangeratesapi.io/ to be set as environment variables.

```bash
source env/bin/activate

export MARKET_STACK_ACCESS_KEY="00000000000000000000000000000000"
export EXCHANGE_RATES_API_ACCESS_KEY="00000000000000000000000000000000"

./load_data.py --symbol AAPL --currency GBP --start-date 2021-11-01 --end-date 2021-11-30
```
