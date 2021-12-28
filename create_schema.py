#!/usr/bin/env python

import logging

from market_data_loader import database, logger, models


def main():
    logger.configure_logger()

    logging.info('Creating the database schema..')

    db_engine = database.create_engine()
    models.Base.metadata.create_all(db_engine)


if __name__ == "__main__":
    main()
