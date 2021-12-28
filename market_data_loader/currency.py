import logging

import pandas as pd

import market_data_loader.clients.exchangeratesapi_client as currency_client
from market_data_loader.models import CurrencyRate


def get_currency_rates(db_sessionmaker, dates, base_currency, target_currency):
    logging.info("Get currency rates from '%s' to '%s'", base_currency, target_currency)

    _fill_missing_dates(db_sessionmaker, dates, base_currency, target_currency)

    return _get_currency_rates(db_sessionmaker, dates, base_currency, target_currency)


def _fill_missing_dates(db_sessionmaker, dates, base_currency, target_currency):
    start_date = min(dates)
    end_date = max(dates)

    target_currency_cached_dates = _query_cached_dates(
        db_sessionmaker,
        currency_client.BASE_CURRENCY,
        target_currency,
        start_date,
        end_date,
    )

    if not target_currency_cached_dates and not _is_valid_currency(target_currency):
        raise RuntimeError(f"Target currency '{target_currency}' is not supported")

    base_currency_cached_dates = _query_cached_dates(
        db_sessionmaker,
        currency_client.BASE_CURRENCY,
        base_currency,
        start_date,
        end_date,
    )

    with db_sessionmaker.begin() as session:
        for date in dates:
            if (
                date in base_currency_cached_dates
                and date in target_currency_cached_dates
            ):
                continue

            currency_rate = currency_client.currency_rate(
                date, [base_currency, target_currency]
            )

            if not date in base_currency_cached_dates:
                session.add(
                    CurrencyRate(
                        date=date,
                        base_currency=currency_rate["base"],
                        target_currency=base_currency,
                        rate=float(currency_rate["rates"][base_currency]),
                    )
                )

            if not date in target_currency_cached_dates:
                session.add(
                    CurrencyRate(
                        date=date,
                        base_currency=currency_rate["base"],
                        target_currency=target_currency,
                        rate=float(currency_rate["rates"][target_currency]),
                    )
                )


def _is_valid_currency(currency):
    currency_codes = currency_client.currencies()
    return currency in currency_codes["symbols"]


def _get_currency_rates(db_sessionmaker, dates, base_currency, target_currency):
    # The currency client that we are using supports only a single base
    # currency. To get the currency rate that we need, we can combine the known
    # rates of the initial and target currency through the base currency.

    start_date = min(dates)
    end_date = max(dates)

    base_currency_rates = _query_currency_rates_as_dataframe(
        db_sessionmaker,
        currency_client.BASE_CURRENCY,
        base_currency,
        start_date,
        end_date,
    )

    base_currency_rates = base_currency_rates[["target_currency", "rate"]].rename(
        columns={"target_currency": "currency"}
    )

    target_currency_rates = _query_currency_rates_as_dataframe(
        db_sessionmaker,
        currency_client.BASE_CURRENCY,
        target_currency,
        start_date,
        end_date,
    )

    target_currency_rates = target_currency_rates[["target_currency", "rate"]].rename(
        columns={"target_currency": "currency"}
    )

    currency_rates = base_currency_rates.join(
        target_currency_rates,
        lsuffix="_base",
        rsuffix="_target",
    )

    currency_rates["rate"] = currency_rates["rate_target"] / currency_rates["rate_base"]

    return currency_rates[["currency_base", "currency_target", "rate"]].rename(
        columns={"currency_base": "base_currency", "currency_target": "target_currency"}
    )


def _query_cached_dates(
    db_sessionmaker, base_currency, target_currency, start_date, end_date
):
    with db_sessionmaker.begin() as session:
        data_rows = (
            session.query(CurrencyRate.date)
            .filter(
                CurrencyRate.base_currency == base_currency,
                CurrencyRate.target_currency == target_currency,
            )
            .filter(CurrencyRate.date >= start_date, CurrencyRate.date <= end_date)
            .all()
        )

        return {row[0] for row in data_rows}


def _query_currency_rates_as_dataframe(
    db_sessionmaker, base_currency, target_currency, start_date, end_date
):
    with db_sessionmaker.begin() as session:
        statement = (
            session.query(CurrencyRate)
            .filter(
                CurrencyRate.base_currency == base_currency,
                CurrencyRate.target_currency == target_currency,
            )
            .filter(CurrencyRate.date >= start_date, CurrencyRate.date <= end_date)
            .statement
        )

        return pd.read_sql(statement, session.bind).set_index("date")
