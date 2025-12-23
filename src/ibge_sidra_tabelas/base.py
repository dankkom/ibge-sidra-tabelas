"""Base classes and helpers for data-loading scripts.

This module defines `BaseScript`, an abstract base that encapsulates a
typical workflow used throughout the project:

- Determine which SIDRA tables to fetch (`get_tabelas`).
- Download table CSVs using the `sidra.Fetcher` helper.
- Refine the raw data (`refine`) and load it into a SQL database.

Concrete scripts should subclass `BaseScript` and implement the
abstract methods: `get_tabelas`, `create_table` and `refine`.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa

from . import database, sidra, storage
from .config import Config

logger = logging.getLogger(__name__)


class BaseScript(ABC):
    """Abstract base for scripts that fetch SIDRA data and load it.

    Subclasses must implement the three abstract methods used to
    declare which tables to fetch, create the target database table,
    and transform raw DataFrames into the shape expected by the
    database.

    Attributes:
        config: A `Config` instance containing database and runtime
            configuration (see `config.Config`).
        fetcher: A `sidra.Fetcher` instance used to download tables.
    """

    def __init__(self, config: Config):
        """Initialize the script with the given configuration.

        Args:
            config: Project configuration including database connection
                parameters and destination table/schema names.
        """
        self.config = config
        self.fetcher = sidra.Fetcher()

    @abstractmethod
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        """Return an iterable of table request definitions.

        Each yielded dictionary must contain the keyword arguments
        accepted by `sidra.Fetcher.download_table` (for example
        ``sidra_tabela``, ``territories``, ``variables``, ``classifications``).

        Returns:
            An iterable of dictionaries describing tables to download.
        """
        pass

    @abstractmethod
    def create_table(self, engine: sa.Engine):
        """Create the destination database table if it does not exist.

        Implementations should use the provided SQLAlchemy ``engine`` to
        execute DDL necessary to create the target schema/table matching
        the structure produced by ``refine``.
        """
        pass

    @abstractmethod
    def refine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform a raw SIDRA DataFrame into the loadable format.

        This method receives the DataFrame returned by
        `storage.read_file` and must return a cleaned/normalized
        DataFrame ready to be written to the configured database table.

        Args:
            df: Raw DataFrame read from the SIDRA CSV.

        Returns:
            A transformed DataFrame ready for ``to_sql``.
        """
        pass

    def download(
        self, tabelas: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Download all tables described by ``tabelas``.

        For each table definition yielded by ``tabelas`` this method
        delegates to the `sidra.Fetcher` to download each period's CSV.
        The returned list contains the original table definition with an
        added ``"filepath"`` key for each downloaded file.

        Args:
            tabelas: Iterable of table-definition dictionaries.

        Returns:
            A list of dictionaries where each entry includes a
            ``"filepath"`` key pointing to the downloaded CSV.
        """
        data_files = []
        for tabela in tabelas:
            _filepaths = self.fetcher.download_table(**tabela)
            for filepath in _filepaths:
                data_files.append(tabela | {"filepath": filepath})
        return data_files

    def load_data(self, engine: sa.Engine, data_files: list[dict[str, Any]]):
        """Load a sequence of downloaded CSVs into the configured DB.

        Each entry in ``data_files`` is expected to contain a
        ``"filepath"`` key pointing to a CSV on disk. The CSV is read
        with ``storage.read_file``, transformed via ``refine`` and
        appended to the destination table using ``pandas.DataFrame.to_sql``.

        Args:
            engine: SQLAlchemy engine connected to the target database.
            data_files: List of dictionaries describing files to load
                (each must include a ``"filepath"`` key).
        """
        for data_file in data_files:
            logger.info("Reading file %s", data_file["filepath"])
            df = storage.read_file(data_file["filepath"])
            df = self.refine(df)
            logger.info("Loading data into %s", self.config.db_table)
            df.to_sql(
                self.config.db_table,
                engine,
                schema=self.config.db_schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

    def run(self):
        """Execute the complete fetch-refine-load pipeline.

        Execution steps:
        1. Use the `sidra.Fetcher` (as a context manager) to download
           all declared tables.
        2. Create or validate the destination database table.
        3. Load the transformed data into the database.
        """
        logger.info("Starting script execution")

        with self.fetcher:
            tabelas = self.get_tabelas()
            data_files = self.download(tabelas)

        engine = database.get_engine(self.config)
        self.create_table(engine)
        self.load_data(engine, data_files)
        logger.info("Script execution finished")
