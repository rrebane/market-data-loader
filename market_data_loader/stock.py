import datetime
import logging

import pandas as pd

import market_data_loader.clients.marketstack_client as stock_client
from market_data_loader.models import ExcludedDate, StockPrice


def get_stock_prices(db_sessionmaker, symbol, start_date, end_date):
    logging.info(
        "Get stock prices for '%s' from '%s' to '%s'", symbol, start_date, end_date
    )

    _fill_missing_dates(db_sessionmaker, symbol, start_date, end_date)

    return _query_stock_prices_as_dataframe(
        db_sessionmaker, symbol, start_date, end_date
    )


def _fill_missing_dates(db_sessionmaker, symbol, start_date, end_date):
    cached_dates = _query_cached_dates(db_sessionmaker, symbol, start_date, end_date)

    excluded_dates = _query_excluded_dates(
        db_sessionmaker, symbol, start_date, end_date
    )

    missing_dates = _compute_missing_dates(
        cached_dates, excluded_dates, start_date, end_date
    )

    if not missing_dates:
        return

    # Various heuristics could be used to avoid re-fetching data that we already
    # have. Use a very simple heuristic for now that just re-fetches all data
    # within the first and last missing date. This should minimize the number of
    # requests made in most cases.
    missing_range_start = min(missing_dates)
    missing_range_end = max(missing_dates)

    for paginated_response in stock_client.end_of_day(
        symbol, missing_range_start, missing_range_end
    ):
        with db_sessionmaker.begin() as session:
            for price in paginated_response:
                date = datetime.datetime.strptime(
                    price["date"], "%Y-%m-%dT%H:%M:%S%z"
                ).date()

                if date in missing_dates:
                    session.add(
                        StockPrice(
                            date=date,
                            symbol=price["symbol"],
                            close_price=price["close"],
                            exchange=price["exchange"],
                        )
                    )

                    missing_dates.remove(date)

    with db_sessionmaker.begin() as session:
        for missing_date in missing_dates:
            # Don't exclude today's date, as the market data might not be
            # available yet.
            if missing_date < datetime.date.today():
                session.add(ExcludedDate(date=missing_date, symbol=symbol))


def _compute_missing_dates(dates, excluded_dates, start_date, end_date):
    missing_dates = (
        pd.date_range(start=start_date, end=end_date, freq="B")
        .difference(dates)
        .to_pydatetime()
        .tolist()
    )

    return {date.date() for date in missing_dates if not date.date() in excluded_dates}


def _query_excluded_dates(db_sessionmaker, symbol, start_date, end_date):
    with db_sessionmaker.begin() as session:
        data_rows = (
            session.query(ExcludedDate.date)
            .filter(ExcludedDate.symbol == symbol)
            .filter(ExcludedDate.date >= start_date, ExcludedDate.date <= end_date)
            .all()
        )

        return {row[0] for row in data_rows}


def _query_cached_dates(db_sessionmaker, symbol, start_date, end_date):
    with db_sessionmaker.begin() as session:
        data_rows = (
            session.query(StockPrice.date)
            .filter(StockPrice.symbol == symbol)
            .filter(StockPrice.date >= start_date, StockPrice.date <= end_date)
            .all()
        )

        return {row[0] for row in data_rows}


def _query_stock_prices_as_dataframe(db_sessionmaker, symbol, start_date, end_date):
    with db_sessionmaker.begin() as session:
        statement = (
            session.query(StockPrice)
            .filter(StockPrice.symbol == symbol)
            .filter(StockPrice.date >= start_date, StockPrice.date <= end_date)
            .statement
        )

        return pd.read_sql(statement, session.bind).set_index("date")
