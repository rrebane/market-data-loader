import sqlalchemy as db
import sqlalchemy.orm as orm

ENGINE_URI = "sqlite:///market_data_loader.db"


def create_engine():
    return db.create_engine(ENGINE_URI)


def create_sessionmaker():
    engine = create_engine()
    return orm.sessionmaker(bind=engine, expire_on_commit=False)
