import logging
import time
from pathlib import Path
from typing import Generator

import httpx
import pandas as pd
from sidra_fetcher.api.agregados import Classificacao
from sidra_fetcher.api.sidra import Parametro
from sidra_fetcher.fetcher import SidraClient

from .storage import get_data_dir, get_filename, write_file

logger = logging.getLogger(__name__)


class Fetcher:
    def __init__(self):
        self.sidra_client = SidraClient(timeout=600)
        self.data_dir = get_data_dir()

    def download_table(
        self,
        sidra_tabela: str,
        territories: dict[str, list[str]],
        variables: list[str] | None = None,
        classifications: dict[str, list[str]] | None = None,
    ) -> list[Path]:
        """Download a SIDRA table in CSV format on temp_dir()

        Args:
            sidra_tabela (str): SIDRA table code
            territories (dict[str, list[str]]): dictionary with territory codes.
                The keys are the territory codes and the values are lists of
                territory IDs. For example, {"6": ["1234567", "6789012"]} for
                the state of São Paulo with two municipalities.
            variables (list[str], optional): list of variables to download.
            classifications (dict, optional): classifications and categories codes.

        Returns:
            list[Path]: list of downloaded files
        """

        if variables is None:
            variables = ["all"]

        if classifications is None:
            metadados = self.sidra_client.get_agregado_metadados(
                int(sidra_tabela)
            )
            classifications = {
                str(classificacao.id): []
                for classificacao in metadados.classificacoes
            }

        filepaths: list[Path] = []
        periodos = self.sidra_client.get_agregado_periodos(
            agregado_id=int(sidra_tabela)
        )
        for periodo in periodos:
            parameter = Parametro(
                agregado=sidra_tabela,
                territorios=territories,
                variaveis=variables,
                periodos=[periodo.id],
                classificacoes=classifications,
                decimais="/d/m",
            )
            filename = get_filename(
                parameter=parameter,
                modification=periodo.modificacao.isoformat(),
            )
            dest_filepath = self.data_dir / f"t-{sidra_tabela}" / filename
            dest_filepath.parent.mkdir(exist_ok=True, parents=True)
            if dest_filepath.exists():
                filepaths.append(dest_filepath)
                logger.warning("File already exists: %s", dest_filepath)
                continue
            logger.info("Downloading %s", filename)
            df = self.get_table(parameter)
            write_file(df=df, dest_filepath=dest_filepath)
            filepaths.append(dest_filepath)
        return filepaths

    def get_table(self, parameter: Parametro) -> pd.DataFrame:
        """Get a table from SIDRA API

        Args:
            parameter (Parametro): Parameter object with the request parameters

        Returns:
            pd.DataFrame: DataFrame with the table data
        """
        url = parameter.url()
        while True:
            try:
                data = self.sidra_client.get(url)
                df = pd.DataFrame(data)
                return df
            except httpx.ReadTimeout as e:
                logger.error("Read timeout while fetching data: %s", e)
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)
            except httpx.RemoteProtocolError as e:
                logger.error("Remote protocol error: %s", e)
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sidra_client.__exit__(exc_type, exc_value, traceback)


def unnest_classificacoes(
    classificacoes: list[Classificacao],
    data: dict[str, list[str]] | None = None,
) -> Generator[dict[str, list[str]], None, None]:
    """Recursively list all classifications and categories"""
    if data is None:
        data: dict[str, list[str]] = {}
    for i, classificacao in enumerate(classificacoes, 1):
        classificacao_id = str(classificacao.id)
        for categoria in classificacao.categorias:
            categoria_id = str(categoria.id)
            if categoria_id == "0":
                continue
            data[classificacao_id] = [categoria_id]
            if len(classificacoes) == 1:
                yield dict(**data)
            else:
                yield from unnest_classificacoes(classificacoes[i:], data)
