"""Base classes and helpers for data-loading scripts.

This module defines `BaseScript`, an abstract base that encapsulates a
typical workflow used throughout the project:

- Determine which SIDRA tables to fetch (`get_tabelas`).
- Download table data and metadata using the `sidra.Fetcher` helper.
- Load metadata into sidra_tabela, localidade and dimensao tables.
- Load data into the dados table.

Concrete scripts should subclass `BaseScript` and implement
the abstract method `get_tabelas`.
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import database, models, sidra
from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)


class BaseScript(ABC):
    """Abstract base for scripts that fetch SIDRA data and load it.

    Subclasses must implement `get_tabelas` to declare which tables
    to fetch.

    Attributes:
        config: A `Config` instance containing database configuration.
        fetcher: A `sidra.Fetcher` instance used to download tables.
    """

    def __init__(self, config: Config, max_workers: int = 4):
        self.config = config
        self.storage = Storage.default()
        self.fetcher = sidra.Fetcher(max_workers=max_workers)

    @abstractmethod
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        """Return an iterable of table request definitions.

        Each yielded dictionary must contain the keyword arguments
        accepted by `sidra.Fetcher.download_table` (for example
        ``sidra_tabela``, ``territories``, ``variables``, ``classifications``).
        """
        pass

    def download(
        self, tabelas: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Download all tables described by ``tabelas``."""
        data_files = []
        for tabela in tabelas:
            _filepaths = self.fetcher.download_table(**tabela)
            for filepath in _filepaths:
                data_files.append(tabela | {"filepath": filepath})
        return data_files

    def load_metadata(
        self, engine: sa.Engine, tabelas: Iterable[dict[str, Any]]
    ):
        """Fetch and save metadata for all unique SIDRA tables."""
        seen: set[str] = set()
        for tabela in tabelas:
            sidra_tabela_id = tabela["sidra_tabela"]
            if sidra_tabela_id in seen:
                continue
            seen.add(sidra_tabela_id)

            metadata_filepath = self.storage.get_metadata_filepath(
                sidra_tabela_id
            )
            if metadata_filepath.exists():
                logger.info(
                    "Reading cached metadata for table %s", sidra_tabela_id
                )
                agregado = self.storage.read_metadata(sidra_tabela_id)
            else:
                logger.info(
                    "Fetching metadata for table %s", sidra_tabela_id
                )
                agregado = self.fetcher.fetch_metadata(sidra_tabela_id)
                self.storage.write_metadata(agregado)

            logger.info(
                "Saving metadata to database for table %s", sidra_tabela_id
            )
            database.save_agregado(engine, agregado)

    def _build_localidade_lookup(
        self, engine: sa.Engine, keys: Iterable[tuple] | None = None
    ) -> dict[tuple, int]:
        """Build a lookup dict from localidade columns to localidade ID."""
        lookup = {}
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
                    chunk = keys[i : i + 1000]
                    chunk_stmt = stmt.where(
                        sa.tuple_(models.Localidade.nc, models.Localidade.d1c).in_(chunk)
                    )
                    for row in conn.execute(chunk_stmt):
                        lookup[(row.nc, row.d1c)] = row.id
            else:
                for row in conn.execute(stmt):
                    lookup[(row.nc, row.d1c)] = row.id
            return lookup

    def _build_dimensao_lookup(
        self, engine: sa.Engine, keys: Iterable[tuple] | None = None
    ) -> dict[tuple, int]:
        """Build a lookup dict from dimension columns to dimensao ID."""
        lookup = {}
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
                d2c_keys = list({k[0] for k in keys if k is not None and k[0] is not None})
                if not d2c_keys:
                    return lookup
                for i in range(0, len(d2c_keys), 1000):
                    chunk = d2c_keys[i : i + 1000]
                    chunk_stmt = stmt.where(models.Dimensao.d2c.in_(chunk))
                    for row in conn.execute(chunk_stmt):
                        key = (
                            row.d2c,
                            row.d4c,
                            row.d5c,
                            row.d6c,
                            row.d7c,
                            row.d8c,
                            row.d9c,
                        )
                        lookup[key] = row.id
            else:
                for row in conn.execute(stmt):
                    key = (
                        row.d2c,
                        row.d4c,
                        row.d5c,
                        row.d6c,
                        row.d7c,
                        row.d8c,
                        row.d9c,
                    )
                    lookup[key] = row.id
            return lookup

    def _upsert_dimensoes_from_data(
        self, engine: sa.Engine, data_files: list[dict[str, Any]]
    ):
        """Insert dimensions from data files into the dimensao table.

        All data files are in Formato.A and carry both codes and names
        (MC, MN, D2C, D2N, etc.). Only dimensions present in the downloaded
        data are inserted.
        """
        key_cols = ["MC", "D2C", "D4C", "D5C", "D6C", "D7C", "D8C", "D9C"]
        name_cols = ["MN", "D2N", "D4N", "D5N", "D6N", "D7N", "D8N", "D9N"]

        for data_file in data_files:
            filepath = data_file["filepath"]

            logger.info("Upserting dimensions from %s", filepath)
            rows = self.storage.read_data(filepath)
            if not rows:
                continue

            seen: set[tuple] = set()
            dim_dicts = []
            for row in rows:
                key = tuple(
                    str(row[c]) if row.get(c) is not None else None
                    for c in key_cols
                )
                if key in seen:
                    continue
                seen.add(key)

                def _s(col):
                    v = row.get(col)
                    return str(v) if v is not None else None

                dim_dicts.append({
                    "mc": _s("MC"),
                    "mn": _s("MN") or "",
                    "d2c": _s("D2C") or "",
                    "d2n": _s("D2N") or "",
                    "d4c": _s("D4C"), "d4n": _s("D4N"),
                    "d5c": _s("D5C"), "d5n": _s("D5N"),
                    "d6c": _s("D6C"), "d6n": _s("D6N"),
                    "d7c": _s("D7C"), "d7n": _s("D7N"),
                    "d8c": _s("D8C"), "d8n": _s("D8N"),
                    "d9c": _s("D9C"), "d9n": _s("D9N"),
                })

            if not dim_dicts:
                continue

            with engine.connect() as conn:
                for i in range(0, len(dim_dicts), 1000):
                    batch = dim_dicts[i : i + 1000]
                    stmt = pg_insert(models.Dimensao.__table__).values(batch)
                    stmt = stmt.on_conflict_do_nothing()
                    conn.execute(stmt)
                conn.commit()

            logger.info("Upserted %d dimensions from %s", len(dim_dicts), filepath)

    def load_data(self, engine: sa.Engine, data_files: list[dict[str, Any]]):
        """Load downloaded data files into the dados table."""
        dim_cols = ["D2C", "D4C", "D5C", "D6C", "D7C", "D8C", "D9C"]

        for data_file in data_files:
            sidra_tabela_id = str(data_file["sidra_tabela"])
            filepath = data_file["filepath"]

            # Extract modification date from filename (after @ before .json)
            modificacao = filepath.stem.split("@")[-1]

            rows = self.storage.read_data(filepath)
            if not rows:
                continue

            # Drop rows where V is missing
            rows = [r for r in rows if r.get("V") is not None]
            if not rows:
                continue

            def clean_str(val):
                if val is None:
                    return ""
                return re.sub(r'\.0$', '', str(val).strip())

            seen_locs = set()
            loc_dicts = []
            loc_keys_list = []

            for r in rows:
                nc = clean_str(r.get("NC"))
                nn = str(r.get("NN", "")).strip()
                d1c = clean_str(r.get("D1C"))
                d1n = str(r.get("D1N", "")).strip()

                loc_keys_list.append((nc, d1c))

                if (nc, d1c) not in seen_locs:
                    seen_locs.add((nc, d1c))
                    loc_dicts.append({
                        "nc": nc,
                        "nn": nn,
                        "d1c": d1c,
                        "d1n": d1n,
                    })

            # Upsert Localidades found in this data file dynamically
            with engine.connect() as conn:
                for i in range(0, len(loc_dicts), 1000):
                    batch = loc_dicts[i : i + 1000]
                    stmt = pg_insert(models.Localidade.__table__).values(batch)
                    stmt = stmt.on_conflict_do_nothing()
                    conn.execute(stmt)
                conn.commit()

            # Rebuild lookup so we have the IDs for newly inserted localidades (just for present keys)
            loc_lookup = self._build_localidade_lookup(engine, keys=seen_locs)

            dim_keys_list = []
            seen_dims = set()
            for r in rows:
                key = tuple(
                    str(r.get(c)) if r.get(c) is not None else None
                    for c in dim_cols
                )
                dim_keys_list.append(key)
                seen_dims.add(key)
                
            dim_lookup = self._build_dimensao_lookup(engine, keys=seen_dims)

            dados_to_insert = []
            missing_dims = 0
            missing_locs = 0

            for i, r in enumerate(rows):
                dim_key = dim_keys_list[i]
                loc_key = loc_keys_list[i]
                
                dim_id = dim_lookup.get(dim_key)
                if dim_id is None:
                    missing_dims += 1
                    continue
                    
                loc_id = loc_lookup.get(loc_key)
                if loc_id is None:
                    missing_locs += 1
                    continue

                dados_to_insert.append({
                    "sidra_tabela_id": sidra_tabela_id,
                    "localidade_id": loc_id,
                    "dimensao_id": dim_id,
                    "d3c": str(r.get("D3C")),
                    "modificacao": modificacao,
                    "ativo": True,
                    "v": str(r.get("V")),
                })

            if missing_dims > 0:
                logger.warning("Skipping %d rows with unknown dimensao in %s", missing_dims, filepath)
            if missing_locs > 0:
                logger.warning("Skipping %d rows with unknown localidade in %s", missing_locs, filepath)

            if not dados_to_insert:
                continue

            logger.info("Loading %d rows into dados", len(dados_to_insert))
            with engine.connect() as conn:
                for i in range(0, len(dados_to_insert), 1000):
                    batch = dados_to_insert[i : i + 1000]
                    stmt = pg_insert(models.Dados.__table__).values(batch)
                    stmt = stmt.on_conflict_do_nothing()
                    conn.execute(stmt)
                conn.commit()

    def run(self):
        """Execute the complete fetch-and-load pipeline.

        Execution steps:
        1. Create ORM tables if they don't exist.
        2. Fetch and save metadata for all declared tables.
        3. Download all data files.
        4. Load data into the dados table.
        """
        logger.info("Starting script execution")

        engine = database.get_engine(self.config)
        models.Base.metadata.create_all(engine)

        tabelas = list(self.get_tabelas())

        with self.fetcher:
            self.load_metadata(engine, tabelas)
            data_files = self.download(tabelas)

        self._upsert_dimensoes_from_data(engine, data_files)
        self.load_data(engine, data_files)
        logger.info("Script execution finished")
