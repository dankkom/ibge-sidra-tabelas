import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import sqlalchemy as sa

from ibge_sidra_tabelas.base import BaseScript


class DummyConfig:
    def __init__(self, table, schema=None):
        self.db_table = table
        self.db_schema = schema


class DummyScript(BaseScript):
    def __init__(self, config):
        super().__init__(config)
        # Replace fetcher in tests as needed

    def get_tabelas(self):
        # Return a single table definition suitable for tests
        return [{"sidra_tabela": "1", "territories": {"6": ["1"]}}]

    def create_table(self, engine: sa.Engine):
        # Create the table if not exists (columns will be inferred by to_sql)
        return None

    def refine(self, df: pd.DataFrame) -> pd.DataFrame:
        # Default behaviour: pass through
        return df


class TestBaseScript(unittest.TestCase):
    def test_download_attaches_filepaths(self):
        cfg = DummyConfig(table="t")
        script = DummyScript(cfg)

        # Provide a fake fetcher that returns a known Path
        class FakeFetcher:
            def download_table(self, **kwargs):
                return [Path("/tmp/f1.csv")]

        script.fetcher = FakeFetcher()

        tabelas = script.get_tabelas()
        data_files = script.download(tabelas)
        self.assertEqual(len(data_files), 1)
        self.assertIn("filepath", data_files[0])

    @patch("ibge_sidra_tabelas.base.storage.read_file")
    def test_load_data_reads_refines_and_writes(self, mock_read):
        cfg = DummyConfig(table="my_table")
        script = DummyScript(cfg)

        # Prepare a DataFrame to be returned by storage.read_file
        df = pd.DataFrame({"col": [1, 2], "val": ["a", "b"]})
        mock_read.return_value = df

        engine = sa.create_engine("sqlite:///:memory:")

        # data_files list with a filepath value (content is mocked)
        data_files = [{"filepath": Path("/does/not/matter.csv")}]

        script.load_data(engine, data_files)

        # Verify table was created and contains two rows
        result = pd.read_sql_table(cfg.db_table, con=engine)
        self.assertEqual(len(result), 2)

    def test_run_integration_calls_all_steps(self):
        cfg = DummyConfig(table="run_table")
        script = DummyScript(cfg)

        # Fake fetcher that acts as a context manager and returns one file
        class FakeFetcherCM:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb):
                return None

            def download_table(self_inner, **kwargs):
                return [Path("/tmp/run.csv")]

        script.fetcher = FakeFetcherCM()

        # Patch storage.read_file so load_data will receive a DataFrame
        with patch("ibge_sidra_tabelas.base.storage.read_file") as mock_read:
            mock_read.return_value = pd.DataFrame({"x": [1]})

            # Patch database.get_engine to use sqlite in-memory
            with patch(
                "ibge_sidra_tabelas.base.database.get_engine"
            ) as mock_get_eng:
                engine = sa.create_engine("sqlite:///:memory:")
                mock_get_eng.return_value = engine

                # Run the script; should not raise
                script.run()

                # Verify the table has one row inserted
                result = pd.read_sql_table(cfg.db_table, con=engine)
                self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
