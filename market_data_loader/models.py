import sqlalchemy as sa
import sqlalchemy.orm as orm

Base = orm.declarative_base()


class CurrencyRate(Base):
    __table__ = sa.Table(
        "currency_rates",
        Base.metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("base_currency", sa.String(3), nullable=False),
        sa.Column("target_currency", sa.String(3), nullable=False),
        sa.Column("rate", sa.Float(), nullable=False),
    )

    sa.Index(
        "currency_rates_date_base_currency_target_currency_index",
        __table__.c.date,
        __table__.c.base_currency,
        __table__.c.target_currency,
        unique=True,
    )

    def __repr__(self):
        return str(self.__dict__)


class ExcludedDate(Base):
    __table__ = sa.Table(
        "excluded_dates",
        Base.metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("symbol", sa.String(15), nullable=False),
    )

    sa.Index(
        "excluded_dates_date_symbol_index",
        __table__.c.date,
        __table__.c.symbol,
        unique=True,
    )

    def __repr__(self):
        return str(self.__dict__)


class StockPrice(Base):
    __table__ = sa.Table(
        "stock_prices",
        Base.metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("symbol", sa.String(15), nullable=False),
        sa.Column("close_price", sa.Float(), nullable=False),
        sa.Column("exchange", sa.String(15), nullable=False),
        sa.Column("currency", sa.String(3), default="USD", nullable=False),
    )

    sa.Index(
        "stock_prices_date_symbol_index",
        __table__.c.date,
        __table__.c.symbol,
        unique=True,
    )

    def __repr__(self):
        return str(self.__dict__)
