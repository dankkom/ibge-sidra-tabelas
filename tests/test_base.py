import unittest
from pathlib import Path

from ibge_sidra_tabelas.base import BaseScript


class DummyConfig:
    def __init__(self, schema=None):
        self.db_schema = schema


class DummyScript(BaseScript):
    def __init__(self, config):
        super().__init__(config)

    def get_tabelas(self):
        return [{"sidra_tabela": "1", "territories": {"6": ["1"]}}]


class TestBaseScript(unittest.TestCase):
    def test_download_attaches_filepaths(self):
        cfg = DummyConfig()
        script = DummyScript(cfg)

        class FakeFetcher:
            def download_table(self, **kwargs):
                return [Path("/tmp/f1.csv")]

        script.fetcher = FakeFetcher()

        tabelas = script.get_tabelas()
        data_files = script.download(tabelas)
        self.assertEqual(len(data_files), 1)
        self.assertIn("filepath", data_files[0])


if __name__ == "__main__":
    unittest.main()
