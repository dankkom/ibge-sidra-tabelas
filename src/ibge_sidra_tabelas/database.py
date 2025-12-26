"""Database helpers: engine creation, loading and DDL/DCL builders.

This module contains utilities used by the project's data-loading
scripts to connect to PostgreSQL, write DataFrames to the database and
generate simple DDL/DCL statements for table creation and permissioning.

Public functions:
- `get_engine`: create a SQLAlchemy engine from `Config`.
- `load`: append a DataFrame to a configured table handling basic
    integrity errors.
- `build_ddl`: build a CREATE TABLE statement string.
- `build_dcl`: build owner/grant statements for a table.
"""

import logging
from typing import Iterable

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .config import Config

logger = logging.getLogger(__name__)


def get_engine(config: Config) -> sa.engine.Engine:
    """Create and return a SQLAlchemy engine for the configured DB.

    The function constructs a PostgreSQL connection string using the
    provided `Config` and returns an engine produced by
    ``sqlalchemy.create_engine``.

    Args:
        config: A `Config` instance with attributes `db_user`,
            `db_password`, `db_host`, `db_port` and `db_name`.

    Returns:
        A `sqlalchemy.engine.Engine` connected to the target database.
    """
    db_user = config.db_user
    db_password = config.db_password
    db_host = config.db_host
    db_port = config.db_port
    db_name = config.db_name
    connection_string = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = sa.create_engine(connection_string)
    return engine


def insert_on_conflict_do_nothing(pd_table, conn, keys, data_iter):
    """Pandas to_sql method that performs an INSERT ON CONFLICT DO NOTHING.

    This is useful for bulk loading where some keys might already exist.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    columns = [sa.Column(k) for k in keys]
    table = sa.Table(pd_table.name, sa.MetaData(), *columns, schema=pd_table.schema)

    stmt = pg_insert(table).values(data)
    stmt = stmt.on_conflict_do_nothing()

    conn.execute(stmt)


def load(df: pd.DataFrame, engine: sa.engine.Engine, config: Config):
    """Append a DataFrame to the configured database table.

    This convenience wrapper logs the operation and calls
    ``pandas.DataFrame.to_sql`` with sensible defaults. Integrity errors
    (for example due to primary key violations) are caught and logged as
    warnings so that callers can continue processing other files.

    Args:
        df: DataFrame to be loaded.
        engine: SQLAlchemy engine connected to the target DB.
        config: Configuration with ``db_table`` and ``db_schema``.
    """
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
    except sqlalchemy.exc.IntegrityError:
        logger.warning(
            "Integrity error: failed to load data into %s table",
            config.db_table,
        )


def build_ddl(
    schema: str,
    table_name: str,
    tablespace: str,
    columns: dict[str, str],
    primary_keys: Iterable[str],
    comment: str = "",
) -> str:
    """Build a CREATE TABLE DDL string.

    Args:
        schema: Target schema name.
        table_name: Target table name.
        tablespace: Tablespace to assign to the table.
        columns: Mapping of column name to SQL type (e.g. ``{"id": "BIGINT"}``).
        primary_keys: Iterable of column names comprising the primary key.
        comment: Optional table comment.

    Returns:
        A string with the SQL DDL to create the table and an optional
        COMMENT statement. The caller is responsible for executing the
        DDL against the database.
    """
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
    """Build DCL statements to set table owner and grant SELECT.

    Args:
        schema: Schema containing the table.
        table_name: Name of the table.
        table_owner: Role/user to set as table owner.
        table_user: Role/user to grant SELECT to.

    Returns:
        A string containing ALTER TABLE and GRANT statements separated
        by a blank line.
    """
    dcl_table_owner = f"ALTER TABLE IF EXISTS {schema}.{table_name} OWNER TO {table_owner};"

    dcl_grant = f"GRANT SELECT ON TABLE {schema}.{table_name} TO {table_user};"

    dcl = "\n\n".join((dcl_table_owner, dcl_grant))

    return dcl
