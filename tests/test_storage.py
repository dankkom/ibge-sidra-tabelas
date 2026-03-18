import tempfile
import unittest

from ibge_sidra_tabelas.storage import Storage


class _Fmt:
    def __init__(self, value: str):
        self.value = value


class _SimpleParam:
    def __init__(
        self,
        agregado: str,
        territorios: dict,
        periodos: list[str],
        variaveis: list | None,
        classificacoes: dict,
        formato: object,
    ):
        self.agregado = agregado
        self.territorios = territorios
        self.periodos = periodos
        self.variaveis = variaveis
        self.classificacoes = classificacoes
        self.formato = formato


class TestStorage(unittest.TestCase):
    def test_get_filename(self):
        parameter = _SimpleParam(
            agregado="123",
            territorios={"6": ["12345", "67890"]},
            periodos=["202001", "202002"],
            variaveis=["allxp"],
            classificacoes={"": []},
            formato=_Fmt("C"),
        )
        modification = "2005-01-05"
        filename = Storage.build_data_filename(parameter, modification)
        expected_filename = (
            "t-123_p-202001,202002_f-C_n6-12345,67890_v-allxp_c-@2005-01-05.json"
        )
        self.assertEqual(filename, expected_filename)

    def test_get_filename_empty_territory_and_no_vars(self):
        parameter = _SimpleParam(
            agregado="999",
            territorios={"6": []},
            periodos=["202101"],
            variaveis=None,
            classificacoes={"10": ["1","2"]},
            formato=_Fmt("C"),
        )
        modification = "2021-01-01"
        filename = Storage.build_data_filename(parameter, modification)
        expected = "t-999_p-202101_f-C_n6-all_c10-1,2@2021-01-01.json"
        self.assertEqual(filename, expected)

    def test_write_and_read_data(self):
        # Create a dict and write it using write_data, then verify reading
        data = [
            {"metadata": "some info"},
            {"V": 1, "Other": "a"},
            {"V": 2, "Other": "b"},
            {"V": "...", "Other": "-"}
        ]

        with tempfile.TemporaryDirectory() as td:
            storage = Storage(td)
            
            # create param
            param = _SimpleParam("1", {"6": ["1"]}, ["2020"], None, {"": []}, _Fmt("C"))
            
            # write
            storage.write_data(data, param, "2005-01-05")
            
            # read_data should handle dropping the 0th row
            filepath = storage.get_data_filepath(param, "2005-01-05")
            cleaned = storage.read_data(filepath)
            
            self.assertEqual(len(cleaned), 3)
            self.assertEqual(cleaned[0]["V"], 1)
            self.assertIsNone(cleaned[2]["V"])
            self.assertIsNone(cleaned[2]["Other"])


if __name__ == "__main__":
    unittest.main()
