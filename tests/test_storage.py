import unittest

from sidra_fetcher.api.sidra import Parametro

from ibge_sidra_tabelas.storage import get_filename


class TestStorage(unittest.TestCase):
    def test_get_filename(self):
        parameter = Parametro(
            agregado="123",
            territorios={"6": ["12345", "67890"]},
            variaveis=["allxp"],
            classificacoes={"": []},
            periodos=["202001", "202002"],
            decimais="/d/m",
        )
        modification = "2005-01-05"
        filename = get_filename(parameter, modification)
        expected_filename = (
            "t-123_p-202001,202002_n6-12345,67890_v-allxp_c-@2005-01-05.csv"
        )
        self.assertEqual(filename, expected_filename)


if __name__ == "__main__":
    unittest.main()
