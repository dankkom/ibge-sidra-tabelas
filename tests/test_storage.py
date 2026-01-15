import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ibge_sidra_tabelas.storage import get_filename, read_file, write_file


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
        filename = get_filename(parameter, modification)
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
        filename = get_filename(parameter, modification)
        expected = "t-999_p-202101_f-C_n6-all_c10-1,2@2021-01-01.json"
        self.assertEqual(filename, expected)

    def test_write_and_read_file_with_metadata(self):
        # Create a DataFrame and write it using write_file, then verify
        # reading via a SIDRA-like CSV (with a metadata first line).
        df = pd.DataFrame({"Valor": [1, 2], "Other": ["a", "b"]})

        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td) / "test.csv"

            # Use write_file to write a plain CSV and verify contents
            write_file(df, tmp_path)
            read_back = pd.read_csv(tmp_path)
            # Ensure the written CSV contains the same data
            pd.testing.assert_frame_equal(df, read_back)

            # Now produce a SIDRA-like CSV: one metadata line then header+rows
            sidra_csv = Path(td) / "sidra.csv"
            with sidra_csv.open("w", encoding="utf-8") as f:
                f.write("metadata: generated\n")
                df.to_csv(f, index=False)

            # read_file should handle the extra metadata line and return the data
            cleaned = read_file(sidra_csv)
            # After read_file's skiprows and dropna, the data should be equal
            # (index may differ; compare values)
            self.assertEqual(cleaned.shape[0], 2)
            self.assertListEqual(list(cleaned["Valor"].astype(int)), [1, 2])


if __name__ == "__main__":
    unittest.main()
