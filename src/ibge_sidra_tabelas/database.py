"""Database helpers: engine creation and data-loading functions.

Public functions:
- `get_engine`: create a SQLAlchemy engine from `Config`.
- `save_agregado`: upsert SIDRA table metadata and localidades.
- `build_localidade_lookup`: query localidade IDs by (nc, d1c) keys.
- `build_dimensao_lookup`: query dimensao IDs by dimension key tuples.
- `upsert_dimensoes`: insert unique dimensions from JSON data files.
- `load_dados`: load data rows into the dados table.
- `build_ddl`: build a CREATE TABLE statement string.
- `build_dcl`: build owner/grant statements for a table.
"""

import itertools
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable

import sqlalchemy as sa
from sidra_fetcher.agregados import Agregado
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import models
from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _coerce(val) -> str | None:
    """Return str(val), or None if val is None."""
    return str(val) if val is not None else None


def _clean_str(val) -> str:
    """Normalize a territory/locality code: strip and remove trailing .0."""
    if val is None:
        return ""
    return re.sub(r'\.0$', '', str(val).strip())


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine(config: Config) -> sa.engine.Engine:
    """Create and return a SQLAlchemy engine for the configured DB."""
    connection_string = (
        f"postgresql+psycopg://{config.db_user}:{config.db_password}"
        f"@{config.db_host}:{config.db_port}/{config.db_name}"
    )
    return sa.create_engine(
        connection_string,
        connect_args={"options": f"-c search_path={config.db_schema}"},
    )


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def save_agregado(engine: sa.engine.Engine, agregado: Agregado):
    """Save metadata to the database (idempotent)."""
    sidra_tabela = dict(
        id=str(agregado.id),
        nome=agregado.nome,
        periodicidade=agregado.periodicidade.frequencia,
        metadados=json.loads(json.dumps(agregado.asdict(), default=str)),
    )
    with engine.connect() as conn:
        stmt = pg_insert(models.SidraTabela.__table__).values(sidra_tabela)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={"metadados": stmt.excluded.metadados},
        )
        conn.execute(stmt)
        conn.commit()

    localidades_iter = (
        dict(
            nc=str(localidade.nivel.id),
            nn=localidade.nivel.nome,
            d1c=str(localidade.id),
            d1n=localidade.nome,
        )
        for localidade in agregado.localidades
    )
    with engine.connect() as conn:
        while True:
            batch = list(itertools.islice(localidades_iter, 1000))
            if not batch:
                break
            stmt = pg_insert(models.Localidade.__table__).values(batch)
            conn.execute(stmt.on_conflict_do_nothing())
            conn.commit()


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def build_localidade_lookup(
    engine: sa.Engine, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (nc, d1c) → localidade.id."""
    lookup: dict[tuple, int] = {}
    with engine.connect() as conn:
        stmt = sa.select(
            models.Localidade.id,
            models.Localidade.nc,
            models.Localidade.d1c,
        )
        if keys is not None:
            keys = list(keys)
            if not keys:
                return lookup
            for i in range(0, len(keys), 1000):
                chunk_stmt = stmt.where(
                    sa.tuple_(models.Localidade.nc, models.Localidade.d1c).in_(
                        keys[i : i + 1000]
                    )
                )
                for row in conn.execute(chunk_stmt):
                    lookup[(row.nc, row.d1c)] = row.id
        else:
            for row in conn.execute(stmt):
                lookup[(row.nc, row.d1c)] = row.id
    return lookup


def build_dimensao_lookup(
    engine: sa.Engine, keys: Iterable[tuple] | None = None
) -> dict[tuple, int]:
    """Return a mapping of (d2c, d4c…d9c) → dimensao.id."""
    lookup: dict[tuple, int] = {}
    with engine.connect() as conn:
        stmt = sa.select(
            models.Dimensao.id,
            models.Dimensao.d2c,
            models.Dimensao.d4c,
            models.Dimensao.d5c,
            models.Dimensao.d6c,
            models.Dimensao.d7c,
            models.Dimensao.d8c,
            models.Dimensao.d9c,
        )
        if keys is not None:
            d2c_keys = list(
                {k[0] for k in keys if k is not None and k[0] is not None}
            )
            if not d2c_keys:
                return lookup
            for i in range(0, len(d2c_keys), 1000):
                chunk_stmt = stmt.where(
                    models.Dimensao.d2c.in_(d2c_keys[i : i + 1000])
                )
                for row in conn.execute(chunk_stmt):
                    lookup[(row.d2c, row.d4c, row.d5c, row.d6c, row.d7c, row.d8c, row.d9c)] = row.id
        else:
            for row in conn.execute(stmt):
                lookup[(row.d2c, row.d4c, row.d5c, row.d6c, row.d7c, row.d8c, row.d9c)] = row.id
    return lookup


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

_DIM_KEY_COLS = ["MC", "D2C", "D4C", "D5C", "D6C", "D7C", "D8C", "D9C"]
_DIM_COLS     = ["D2C", "D4C", "D5C", "D6C", "D7C", "D8C", "D9C"]


def upsert_dimensoes(
    engine: sa.Engine,
    storage: Storage,
    tabelas: Iterable[dict[str, Any]],
):
    """Insert unique dimensions from each table's JSON data files.

    For each unique SIDRA table, scans its data directory for all JSON
    data files (excluding metadados.json) and upserts unique dimension
    rows. Only rows with an actual value (V is not None) are considered,
    so rows with no unit information are never inserted.
    """
    seen_tables: set[str] = set()
    for tabela in tabelas:
        sidra_tabela_id = str(tabela["sidra_tabela"])
        if sidra_tabela_id in seen_tables:
            continue
        seen_tables.add(sidra_tabela_id)

        table_dir = storage.data_dir / f"t-{sidra_tabela_id}"
        if not table_dir.exists():
            logger.warning("Data directory not found: %s", table_dir)
            continue

        filepaths = [
            f for f in table_dir.glob("*.json")
            if f.name != "metadados.json"
        ]

        seen: set[tuple] = set()
        total = 0
        for filepath in filepaths:
            rows = storage.read_data(filepath)
            dim_dicts = []
            for row in rows:
                if row.get("V") is None:
                    continue
                key = tuple(
                    _coerce(row.get(c)) for c in _DIM_KEY_COLS
                )
                if key in seen:
                    continue
                seen.add(key)
                dim_dicts.append({
                    "mc":  _coerce(row.get("MC")),
                    "mn":  _coerce(row.get("MN")) or "",
                    "d2c": _coerce(row.get("D2C")) or "",
                    "d2n": _coerce(row.get("D2N")) or "",
                    "d4c": _coerce(row.get("D4C")), "d4n": _coerce(row.get("D4N")),
                    "d5c": _coerce(row.get("D5C")), "d5n": _coerce(row.get("D5N")),
                    "d6c": _coerce(row.get("D6C")), "d6n": _coerce(row.get("D6N")),
                    "d7c": _coerce(row.get("D7C")), "d7n": _coerce(row.get("D7N")),
                    "d8c": _coerce(row.get("D8C")), "d8n": _coerce(row.get("D8N")),
                    "d9c": _coerce(row.get("D9C")), "d9n": _coerce(row.get("D9N")),
                })
            del rows

            if not dim_dicts:
                continue

            with engine.connect() as conn:
                for i in range(0, len(dim_dicts), 1000):
                    stmt = pg_insert(models.Dimensao.__table__).values(dim_dicts[i : i + 1000])
                    conn.execute(stmt.on_conflict_do_nothing())
                conn.commit()
            total += len(dim_dicts)

        logger.info("Upserted %d dimensions for table %s", total, sidra_tabela_id)


def load_dados(
    engine: sa.Engine,
    storage: Storage,
    data_files: list[dict[str, Any]],
):
    """Load data rows from JSON files into the dados table."""
    for data_file in data_files:
        sidra_tabela_id = str(data_file["sidra_tabela"])
        filepath = data_file["filepath"]
        modificacao = filepath.stem.split("@")[-1]

        rows = storage.read_data(filepath)
        if not rows:
            continue

        # Single pass: collect unique locs/dims and compact row tuples
        seen_locs: set[tuple] = set()
        loc_dicts: list[dict] = []
        seen_dims: set[tuple] = set()
        processed: list[tuple] = []  # (loc_key, dim_key, d3c, v)

        for r in rows:
            if r.get("V") is None:
                continue

            nc = _clean_str(r.get("NC"))
            d1c = _clean_str(r.get("D1C"))
            loc_key = (nc, d1c)

            if loc_key not in seen_locs:
                seen_locs.add(loc_key)
                loc_dicts.append({
                    "nc": nc,
                    "nn": str(r.get("NN", "")).strip(),
                    "d1c": d1c,
                    "d1n": str(r.get("D1N", "")).strip(),
                })

            dim_key = tuple(_coerce(r.get(c)) for c in _DIM_COLS)
            seen_dims.add(dim_key)
            processed.append((loc_key, dim_key, str(r.get("D3C")), str(r.get("V"))))

        del rows

        if not processed:
            continue

        # Upsert localidades
        with engine.connect() as conn:
            for i in range(0, len(loc_dicts), 1000):
                stmt = pg_insert(models.Localidade.__table__).values(loc_dicts[i : i + 1000])
                conn.execute(stmt.on_conflict_do_nothing())
            conn.commit()

        loc_lookup = build_localidade_lookup(engine, keys=seen_locs)
        dim_lookup = build_dimensao_lookup(engine, keys=seen_dims)

        missing_dims = 0
        missing_locs = 0
        n_inserted = 0
        batch: list[dict] = []

        with engine.connect() as conn:
            for loc_key, dim_key, d3c, v in processed:
                dim_id = dim_lookup.get(dim_key)
                if dim_id is None:
                    missing_dims += 1
                    continue
                loc_id = loc_lookup.get(loc_key)
                if loc_id is None:
                    missing_locs += 1
                    continue

                batch.append({
                    "sidra_tabela_id": sidra_tabela_id,
                    "localidade_id": loc_id,
                    "dimensao_id": dim_id,
                    "d3c": d3c,
                    "modificacao": modificacao,
                    "ativo": True,
                    "v": v,
                })

                if len(batch) >= 1000:
                    conn.execute(pg_insert(models.Dados.__table__).values(batch).on_conflict_do_nothing())
                    n_inserted += len(batch)
                    batch.clear()

            if batch:
                conn.execute(pg_insert(models.Dados.__table__).values(batch).on_conflict_do_nothing())
                n_inserted += len(batch)
            conn.commit()

        if missing_dims > 0:
            logger.warning("Skipping %d rows with unknown dimensao in %s", missing_dims, filepath)
        if missing_locs > 0:
            logger.warning("Skipping %d rows with unknown localidade in %s", missing_locs, filepath)
        logger.info("Loaded %d rows into dados from %s", n_inserted, filepath)


# ---------------------------------------------------------------------------
# DDL / DCL builders
# ---------------------------------------------------------------------------

def build_ddl(
    schema: str,
    table_name: str,
    tablespace: str,
    columns: dict[str, str],
    primary_keys: Iterable[str],
    comment: str = "",
) -> str:
    """Build a CREATE TABLE DDL string."""
    table_definition = ", ".join(
        f"{col} {typ}" for col, typ in columns.items()
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
        ddl_comment_table = f"COMMENT ON TABLE {schema}.{table_name} IS '{comment}';"

    return "\n\n".join((ddl_create_table, ddl_comment_table))


def build_dcl(
    schema: str,
    table_name: str,
    table_owner: str,
    table_user: str,
) -> str:
    """Build DCL statements to set table owner and grant SELECT."""
    dcl_table_owner = (
        f"ALTER TABLE IF EXISTS {schema}.{table_name} OWNER TO {table_owner};"
    )
    dcl_grant = f"GRANT SELECT ON TABLE {schema}.{table_name} TO {table_user};"
    return "\n\n".join((dcl_table_owner, dcl_grant))
