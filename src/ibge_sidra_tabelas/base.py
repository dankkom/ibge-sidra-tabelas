import logging
from abc import ABC, abstractmethod
from typing import Any, Iterable

import pandas as pd
import sqlalchemy as sa

from . import database, sidra, storage
from .config import Config

logger = logging.getLogger(__name__)


class BaseScript(ABC):
    def __init__(self, config: Config):
        self.config = config
        self.fetcher = sidra.Fetcher()

    @abstractmethod
    def get_tabelas(self) -> Iterable[dict[str, Any]]:
        """Return a list of table definitions to download."""
        pass

    @abstractmethod
    def create_table(self, engine: sa.Engine):
        """Create the database table."""
        pass

    @abstractmethod
    def refine(self, df: pd.DataFrame) -> pd.DataFrame:
        """Refine the dataframe before loading into the database."""
        pass

    def download(
        self, tabelas: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Download tables from SIDRA."""
        data_files = []
        for tabela in tabelas:
            _filepaths = self.fetcher.download_table(**tabela)
            for filepath in _filepaths:
                data_files.append(tabela | {"filepath": filepath})
        return data_files

    def load_data(self, engine: sa.Engine, data_files: list[dict[str, Any]]):
        """Load downloaded data into the database."""
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
        """Main execution flow."""
        logger.info("Starting script execution")

        with self.fetcher:
            tabelas = self.get_tabelas()
            data_files = self.download(tabelas)

        engine = database.get_engine(self.config)
        self.create_table(engine)
        self.load_data(engine, data_files)
        logger.info("Script execution finished")
