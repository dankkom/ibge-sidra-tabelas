"""Base class for SIDRA data-loading scripts.

Concrete scripts subclass `BaseScript` and implement `get_tabelas` to
declare which SIDRA tables to fetch.  `BaseScript.run` then drives the
full pipeline:

1. Create ORM tables if they don't exist.
2. Fetch and save metadata (sidra_tabela, localidade).
3. Download all data files.
4. Upsert dimensions from the downloaded data.
5. Load data rows into the dados table.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Iterable

import sqlalchemy as sa

from . import database, models, sidra
from .config import Config
from .storage import Storage

logger = logging.getLogger(__name__)


class BaseScript(ABC):
    """Abstract base for scripts that fetch SIDRA data and load it."""

    def __init__(self, config: Config, max_workers: int = 4):
        self.config = config
        self.storage = Storage.default(config)
        self.fetcher = sidra.Fetcher(config, max_workers=max_workers)

    @abstractmethod
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        """Return an iterable of table request definitions.

        Each yielded dict must contain the keyword arguments accepted by
        `sidra.Fetcher.download_table` (e.g. ``sidra_tabela``,
        ``territories``, ``variables``, ``classifications``).
        """

    def download(
        self, tabelas: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Download all tables and return a list of data-file descriptors."""
        data_files = []
        for tabela in tabelas:
            for filepath in self.fetcher.download_table(**tabela):
                data_files.append(tabela | {"filepath": filepath})
        return data_files

    def load_metadata(
        self, engine: sa.Engine, tabelas: Iterable[dict[str, Any]]
    ):
        """Fetch and persist metadata for all unique SIDRA tables."""
        seen: set[str] = set()
        for tabela in tabelas:
            sidra_tabela_id = tabela["sidra_tabela"]
            if sidra_tabela_id in seen:
                continue
            seen.add(sidra_tabela_id)

            metadata_filepath = self.storage.get_metadata_filepath(sidra_tabela_id)
            if metadata_filepath.exists():
                logger.info("Reading cached metadata for table %s", sidra_tabela_id)
                agregado = self.storage.read_metadata(sidra_tabela_id)
            else:
                logger.info("Fetching metadata for table %s", sidra_tabela_id)
                agregado = self.fetcher.fetch_metadata(sidra_tabela_id)
                self.storage.write_metadata(agregado)

            logger.info("Saving metadata to database for table %s", sidra_tabela_id)
            database.save_agregado(engine, agregado)

    def run(self):
        """Execute the full fetch-and-load pipeline."""
        logger.info("Starting script execution")

        engine = database.get_engine(self.config)
        models.Base.metadata.create_all(engine)

        tabelas = list(self.get_tabelas())

        with self.fetcher:
            self.load_metadata(engine, tabelas)
            data_files = self.download(tabelas)

        database.upsert_dimensoes(engine, self.storage, tabelas)
        database.load_dados(engine, self.storage, data_files)

        logger.info("Script execution finished")
