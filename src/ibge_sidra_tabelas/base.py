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
from abc import ABC, abstractmethod
from typing import Any, Iterable

import pandas as pd
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
        self, engine: sa.Engine
    ) -> dict[tuple, int]:
        """Build a lookup dict from localidade columns to localidade ID."""
        with engine.connect() as conn:
            result = conn.execute(
                sa.select(
                    models.Localidade.id,
                    models.Localidade.nc,
                    models.Localidade.d1c,
                )
            )
            lookup = {}
            for row in result:
                key = (row.nc, row.d1c)
                lookup[key] = row.id
            return lookup

    def _build_dimensao_lookup(
        self, engine: sa.Engine
    ) -> dict[tuple, int]:
        """Build a lookup dict from dimension columns to dimensao ID."""
        with engine.connect() as conn:
            result = conn.execute(
                sa.select(
                    models.Dimensao.id,
                    models.Dimensao.d2c,
                    models.Dimensao.d4c,
                    models.Dimensao.d5c,
                    models.Dimensao.d6c,
                    models.Dimensao.d7c,
                    models.Dimensao.d8c,
                    models.Dimensao.d9c,
                )
            )
            lookup = {}
            for row in result:
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

    def _update_dimensao_mc(
        self, engine: sa.Engine, data_files: list[dict[str, Any]]
    ):
        """Extract MC (unit code) from Formato.A files and update dimensao."""
        dim_cols = ["D2C", "D4C", "D5C", "D6C", "D7C", "D8C", "D9C"]
        processed_tables = set()

        for data_file in data_files:
            sidra_tabela_id = str(data_file["sidra_tabela"])
            if sidra_tabela_id in processed_tables:
                continue

            filepath = data_file["filepath"]
            # Only process Formato.A files
            if "_f-a_" not in filepath.name:
                continue

            processed_tables.add(sidra_tabela_id)
            logger.info("Extracting MC from %s", filepath)

            df = self.storage.read_data(filepath)
            if df.empty or "MC" not in df.columns:
                continue

            # Ensure dimension columns exist
            for col in dim_cols:
                if col not in df.columns:
                    df[col] = None

            # Get unique (D2C, D4C-D9C, MC) combinations
            seen: set[tuple] = set()
            with engine.connect() as conn:
                for _, row in df.iterrows():
                    mc = row.get("MC")
                    if pd.isna(mc):
                        continue
                    mc = str(mc)

                    key_vals = tuple(
                        str(row[c]) if pd.notna(row.get(c)) else None
                        for c in dim_cols
                    )
                    if key_vals in seen:
                        continue
                    seen.add(key_vals)

                    # Build WHERE clause for matching dimensao row
                    conditions = []
                    for col_name, val in zip(dim_cols, key_vals):
                        col = getattr(models.Dimensao, col_name.lower())
                        if val is None:
                            conditions.append(col.is_(None))
                        else:
                            conditions.append(col == val)

                    stmt = (
                        sa.update(models.Dimensao)
                        .where(*conditions)
                        .values(mc=mc)
                    )
                    conn.execute(stmt)
                conn.commit()

            logger.info(
                "Updated MC for %d dimension combinations in table %s",
                len(seen),
                sidra_tabela_id,
            )

    def load_data(self, engine: sa.Engine, data_files: list[dict[str, Any]]):
        """Load downloaded data files into the dados table."""
        dim_cols = ["D2C", "D4C", "D5C", "D6C", "D7C", "D8C", "D9C"]

        # Load global localidade lookup
        loc_lookup = self._build_localidade_lookup(engine)
        # Load global dimensao lookup
        dim_lookup = self._build_dimensao_lookup(engine)

        for data_file in data_files:
            sidra_tabela_id = str(data_file["sidra_tabela"])
            filepath = data_file["filepath"]

            # Extract modification date from filename (after @ before .json)
            modificacao = filepath.stem.split("@")[-1]

            logger.info("Reading file %s", filepath)
            df = self.storage.read_data(filepath)
            if df.empty:
                continue

            # Drop rows where V is missing
            df = df.dropna(subset=["V"])
            if df.empty:
                continue

            # Upsert Localidades found in this data file dynamically
            for missing_col in ["NN", "D1N"]:
                if missing_col not in df.columns:
                    df[missing_col] = ""
            locs = df[["NC", "NN", "D1C", "D1N"]].drop_duplicates().copy()
            locs = locs.rename(columns={"NC": "nc", "NN": "nn", "D1C": "d1c", "D1N": "d1n"})

            locs["nc"] = locs["nc"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            locs["nn"] = locs["nn"].astype(str).str.strip()
            locs["d1c"] = locs["d1c"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            locs["d1n"] = locs["d1n"].astype(str).str.strip()

            locs.to_sql(
                "localidade",
                engine,
                schema=self.config.db_schema,
                if_exists="append",
                index=False,
                method=database.insert_on_conflict_do_nothing,
                chunksize=1000,
            )

            # Rebuild lookup so we have the IDs for newly inserted localidades
            loc_lookup = self._build_localidade_lookup(engine)

            # Ensure dimension columns exist (fill missing with None)
            for col in dim_cols:
                if col not in df.columns:
                    df[col] = None

            # Build dimension lookup key for each row
            def _make_key(row):
                return tuple(
                    str(row[c]) if pd.notna(row[c]) else None
                    for c in dim_cols
                )

            df["_dim_key"] = df.apply(_make_key, axis=1)
            df["dimensao_id"] = df["_dim_key"].map(dim_lookup)

            # Skip rows with unknown dimensao
            missing = df["dimensao_id"].isna()
            if missing.any():
                logger.warning(
                    "Skipping %d rows with unknown dimensao in %s",
                    missing.sum(),
                    filepath,
                )
                df = df[~missing]

            if df.empty:
                continue

            # Build localidade lookup key
            df["_loc_key"] = list(
                zip(
                    df["NC"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip(),
                    df["D1C"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip(),
                )
            )
            df["localidade_id"] = df["_loc_key"].map(loc_lookup)

            # Skip rows with unknown localidade
            missing_loc = df["localidade_id"].isna()
            if missing_loc.any():
                logger.warning(
                    "Skipping %d rows with unknown localidade in %s",
                    missing_loc.sum(),
                    filepath,
                )
                df = df[~missing_loc]

            if df.empty:
                continue

            # Build dados DataFrame
            dados_df = pd.DataFrame(
                {
                    "sidra_tabela_id": sidra_tabela_id,
                    "localidade_id": df["localidade_id"].astype(int),
                    "dimensao_id": df["dimensao_id"].astype(int),
                    "d3c": df["D3C"].astype(str),
                    "modificacao": modificacao,
                    "ativo": True,
                    "v": df["V"].astype(str),
                }
            )

            logger.info("Loading %d rows into dados", len(dados_df))
            dados_df.to_sql(
                "dados",
                engine,
                schema=self.config.db_schema,
                if_exists="append",
                index=False,
                method=database.insert_on_conflict_do_nothing,
                chunksize=1000,
            )

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

        self._update_dimensao_mc(engine, data_files)
        self.load_data(engine, data_files)
        logger.info("Script execution finished")
