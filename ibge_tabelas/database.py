import logging

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc

from .config import Config

logger = logging.getLogger(__name__)


def get_engine(config: Config) -> sa.engine.Engine:
    db_user = config.db_user
    db_password = config.db_password
    db_host = config.db_host
    db_port = config.db_port
    db_name = config.db_name
    connection_string = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = sa.create_engine(connection_string)
    return engine


def load(df: pd.DataFrame, engine: sa.engine.Engine, config: Config):
    try:
        df.to_sql(
            config.db_table,
            engine,
            schema=config.db_schema,
            if_exists="append",
            index=False,
            chunksize=1_000,
        )
    except sqlalchemy.exc.IntegrityError as e:
        logger.warning("Integrity error> failed loading data into %s table", config.db_table)
