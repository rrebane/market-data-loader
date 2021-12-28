#!/usr/bin/env python

import argparse
import datetime
import logging
import sys

import requests

from market_data_loader import currency, database, logger, stock


def parse_date(arg):
    try:
        return datetime.datetime.strptime(arg, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("invalid date value")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Fetch and display stock prices in specified currency"
    )
    parser.add_argument("--symbol", help="stock symbol ('AAPL')", required=True)
    parser.add_argument("--currency", help="currency symbol ('USD')", required=True)
    parser.add_argument(
        "--start-date",
        default=datetime.date.today(),
        help="start date ('YYYY-mm-dd', default: today's date)",
        type=parse_date,
    )
    parser.add_argument(
        "--end-date",
        default=datetime.date.today(),
        help="end date ('YYYY-mm-dd', default: today's date)",
        type=parse_date,
    )
    parser.add_argument(
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="verbose logging",
    )
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("Start date must be before end date")

    return args


def main():
    args = parse_arguments()

    if args.verbose:
        logger.configure_logger(level=logging.DEBUG)
    else:
        logger.configure_logger(level=logging.INFO)

    try:
        start_date = min(args.start_date, datetime.date.today())
        end_date = min(args.end_date, datetime.date.today())

        db_sessionmaker = database.create_sessionmaker()

        stock_prices_df = stock.get_stock_prices(
            db_sessionmaker, args.symbol, start_date, end_date
        )

        num_rows, _ = stock_prices_df.shape

        if num_rows > 0:
            base_currency = stock_prices_df["currency"][0]
            target_currency = args.currency

            if base_currency != target_currency:
                currency_rates_df = currency.get_currency_rates(
                    db_sessionmaker,
                    list(stock_prices_df.index),
                    base_currency,
                    target_currency,
                )

                stock_prices_df = stock_prices_df.join(currency_rates_df[["rate"]])

                stock_prices_df["currency"] = target_currency
                stock_prices_df["close_price"] = (
                    stock_prices_df["close_price"] * stock_prices_df["rate"]
                )

            print(stock_prices_df[["symbol", "currency", "close_price"]])
        else:
            logging.info("No data for symbol and date range")
    except RuntimeError as err:
        logging.error(err)

        sys.exit(1)


if __name__ == "__main__":
    main()
