import logging
from typing import Iterable

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
    logger.info("Loading data into %s table", config.db_table)
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


def build_ddl(
    schema: str,
    table_name: str,
    tablespace: str,
    columns: dict[str, str],
    primary_keys: Iterable[str],
    comment: str = None,
) -> str:
    table_definition = ", ".join([f"{column_name} {column_type}" for column_name, column_type in columns.items()])
    primary_keys_definition = ", ".join(primary_keys)

    ddl_create_table = (
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n"
        f"    {table_definition},\n"
        f"    CONSTRAINT {table_name}_pkey PRIMARY KEY ({primary_keys_definition})\n"
        ")\n"
        f"TABLESPACE {tablespace};"
    )

    ddl_comment_table = ""
    if comment:
        ddl_comment_table = f"COMMENT ON TABLE {schema}.{table_name} IS '{comment}';"

    ddl = "\n\n".join((ddl_create_table, ddl_comment_table))

    return ddl


def build_dcl(
    schema: str,
    table_name: str,
    table_owner: str,
    table_user: str,
) -> str:
    dcl_table_owner = f"ALTER TABLE IF EXISTS {schema}.{table_name} OWNER TO {table_owner};"

    dcl_grant = f"GRANT SELECT ON TABLE {schema}.{table_name} TO {table_user};"

    dcl = "\n\n".join((dcl_table_owner, dcl_grant))

    return dcl
