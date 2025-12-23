import unittest

import httpx
import pandas as pd

from ibge_sidra_tabelas.sidra import Fetcher, unnest_classificacoes


class _Cat:
    def __init__(self, id):
        self.id = id


class _Cls:
    def __init__(self, id, categorias):
        self.id = id
        self.categorias = [_Cat(c) for c in categorias]


class TestSidra(unittest.TestCase):
    def test_unnest_classificacoes_nested(self):
        # Two-level classification: first has two categories, second has one
        classificacoes = [_Cls(1, [10, 20]), _Cls(2, [100])]
        combos = list(unnest_classificacoes(classificacoes))
        expected = [{"1": ["10"], "2": ["100"]}, {"1": ["20"], "2": ["100"]}]
        self.assertEqual(combos, expected)

    def test_unnest_classificacoes_skips_zero(self):
        classificacoes = [_Cls(1, [0, 5])]
        combos = list(unnest_classificacoes(classificacoes))
        expected = [{"1": ["5"]}]
        self.assertEqual(combos, expected)

    def test_get_table_retries_on_timeout(self):
        fetcher = Fetcher()

        # Replace sidra_client with a fake that fails once then returns data
        calls = {"n": 0}

        class FakeClient:
            def get(self_inner, url):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise httpx.ReadTimeout("timeout")
                return [{"col": 1}, {"col": 2}]

        fetcher.sidra_client = FakeClient()

        # Create a minimal parameter-like object with a url() method
        class P:
            def url(self):
                return "http://example"

        # Patch sleep to no-op to keep test fast
        import ibge_sidra_tabelas.sidra as sidra_module

        orig_sleep = sidra_module.time.sleep
        sidra_module.time.sleep = lambda s: None
        try:
            df = fetcher.get_table(P())
        finally:
            sidra_module.time.sleep = orig_sleep

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_context_manager_exit_delegates(self):
        fetcher = Fetcher()

        class FakeClient:
            def __init__(self):
                self.exited = False

            def __exit__(self, exc_type, exc_value, traceback):
                self.exited = True

        client = FakeClient()
        fetcher.sidra_client = client
        # Call exit and ensure it delegates
        fetcher.__exit__(None, None, None)
        self.assertTrue(client.exited)


if __name__ == "__main__":
    unittest.main()
