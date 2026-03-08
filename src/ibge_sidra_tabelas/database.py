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

import itertools
import logging
from typing import Iterable

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
from sidra_fetcher.agregados import Agregado
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ibge_sidra_tabelas.models import Dimensao, Localidade, SidraTabela

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
    db_schema = config.db_schema
    connection_string = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = sa.create_engine(
        connection_string,
        connect_args={"options": f"-c search_path={db_schema}"},
    )
    return engine


def insert_on_conflict_do_nothing(pd_table, conn, keys, data_iter):
    """Pandas to_sql method that performs an INSERT ON CONFLICT DO NOTHING.

    This is useful for bulk loading where some keys might already exist.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    columns = [sa.Column(k) for k in keys]
    table = sa.Table(
        pd_table.name,
        sa.MetaData(),
        *columns,
        schema=pd_table.schema,
    )

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
    table_definition = ", ".join(
        [
            f"{column_name} {column_type}"
            for column_name, column_type in columns.items()
        ]
    )
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
        ddl_comment_table = (
            f"COMMENT ON TABLE {schema}.{table_name} IS '{comment}';"
        )

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
    dcl_table_owner = (
        f"ALTER TABLE IF EXISTS {schema}.{table_name} OWNER TO {table_owner};"
    )

    dcl_grant = f"GRANT SELECT ON TABLE {schema}.{table_name} TO {table_user};"

    dcl = "\n\n".join((dcl_table_owner, dcl_grant))

    return dcl


def save_agregado(engine: sa.engine.Engine, agregado: Agregado):
    """Save metadata to the database."""

    sidra_tabela = dict(
        id=agregado.id,
        nome=agregado.nome,
        periodicidade=agregado.periodicidade.frequencia,
    )
    # Insert SidraTabela
    with engine.connect() as conn:
        conn.execute(sa.insert(SidraTabela).values(sidra_tabela))
        conn.commit()

    localidades = []
    for localidade in agregado.localidades:
        localidades.append(
            dict(
                sidra_tabela_id=agregado.id,
                nc=localidade.nivel.id,
                nn=localidade.nivel.nome,
                d1c=localidade.id,
                d1n=localidade.nome,
            )
        )
    # Insert Localidade
    with engine.connect() as conn:
        conn.execute(sa.insert(Localidade).values(localidades))
        conn.commit()

    def unnest_dimensoes(
        agregado_id: int,
        variaveis: list,
        classificacoes: list,
    ) -> list[dict]:
        """Expand variables × classification categories into flat Dimensao rows.

        For each variable, computes the cartesian product of all categories
        across every classification (up to 6, mapped to d4–d9).  The unit of
        measure (``mc``/``mn``) is resolved with the following precedence:

        1. The category's own ``unidade`` field (when not ``None``).
        2. The variable's ``unidade`` field as a fallback.

        Args:
            agregado_id: Primary key of the parent ``SidraTabela`` row.
            variaveis: Iterable of :class:`~sidra_fetcher.agregados.Variavel`.
            classificacoes: Iterable of
                :class:`~sidra_fetcher.agregados.Classificacao`.

        Returns:
            A list of :class:`~ibge_sidra_tabelas.models.Dimensao` instances,
            one per (variavel, combination-of-categories) tuple.
        """

        # Pre-build a list of (categoria_list,) per classificacao so that
        # itertools.product can expand them correctly.
        cats_per_classificacao = [
            classificacao.categorias for classificacao in classificacoes
        ]

        # Pad slots d4–d9: the model supports up to 6 classifications.
        MAX_CLASSIFICACOES = 6

        rows: list[Dimensao] = []

        for variavel in variaveis:
            variavel_id = str(variavel.id)
            variavel_nome = variavel.nome
            unidade_nome = variavel.unidade

            if not cats_per_classificacao:
                # No classifications: one row per variable with null d4–d9.
                rows.append(
                    dict(
                        sidra_tabela_id=str(agregado_id),
                        mc=None,
                        mn=unidade_nome or "",
                        d2c=variavel_id,
                        d2n=variavel_nome,
                        d4c=None,
                        d4n=None,
                        d5c=None,
                        d5n=None,
                        d6c=None,
                        d6n=None,
                        d7c=None,
                        d7n=None,
                        d8c=None,
                        d8n=None,
                        d9c=None,
                        d9n=None,
                    )
                )
                continue

            # Cartesian product across all classifications.
            for combo in itertools.product(*cats_per_classificacao):
                # Resolve unit: first category that provides one wins;
                # fall back to the variable's own unit.
                unidade = unidade_nome
                for cat in combo:
                    if cat.unidade is not None:
                        unidade = cat.unidade
                        break

                # Map combo slots → d4…d9 (pad with None when fewer than 6).
                padded = list(combo) + [None] * (
                    MAX_CLASSIFICACOES - len(combo)
                )

                def _id(cat):
                    return str(cat.id) if cat is not None else None

                def _nome(cat):
                    return cat.nome if cat is not None else None

                rows.append(
                    dict(
                        sidra_tabela_id=str(agregado_id),
                        mc=unidade or "",
                        mn=unidade or "",
                        d2c=variavel_id,
                        d2n=variavel_nome,
                        d4c=_id(padded[0]),
                        d4n=_nome(padded[0]),
                        d5c=_id(padded[1]),
                        d5n=_nome(padded[1]),
                        d6c=_id(padded[2]),
                        d6n=_nome(padded[2]),
                        d7c=_id(padded[3]),
                        d7n=_nome(padded[3]),
                        d8c=_id(padded[4]),
                        d8n=_nome(padded[4]),
                        d9c=_id(padded[5]),
                        d9n=_nome(padded[5]),
                    )
                )

        return rows

    dimensoes = unnest_dimensoes(
        agregado_id=agregado.id,
        variaveis=agregado.variaveis,
        classificacoes=agregado.classificacoes,
    )
    # Insert Dimensao
    with engine.connect() as conn:
        conn.execute(sa.insert(Dimensao).values(dimensoes))
        conn.commit()
