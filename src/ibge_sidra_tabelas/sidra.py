"""Utilities to fetch and store SIDRA tables.

This module provides a `Fetcher` context-managed helper that wraps the
`sidra_fetcher` client to download SIDRA tables as CSV-backed pandas
DataFrames and write them to the project's data directory. It also
contains a helper `unnest_classificacoes` which expands nested
classification/category combinations into flat dictionaries suitable for
request parameters.

Public API
- `Fetcher`: context-managed client for downloading SIDRA tables.
- `unnest_classificacoes`: yields classification/category mappings.
"""

import logging
import time
from pathlib import Path
from typing import Generator

import httpx
import pandas as pd
from sidra_fetcher.agregados import Classificacao
from sidra_fetcher.fetcher import SidraClient
from sidra_fetcher.sidra import Formato, Parametro, Precisao

from .storage import get_data_dir, get_filename, write_json

logger = logging.getLogger(__name__)


class Fetcher:
    """Helper to download SIDRA tables and save them locally.

    This class wraps a `SidraClient` to provide higher-level operations
    to download all periods of a given SIDRA table, write each period's
    result to disk and return the written file paths.

    Usage example::

        with Fetcher() as f:
            files = f.download_table(...)

    Attributes:
        sidra_client: An instance of `SidraClient` used to perform HTTP
            requests to the SIDRA API.
        data_dir: Base directory where downloaded files will be stored.
    """

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
        """Download all periods of a SIDRA table and save them to disk.

        For each period returned by the SIDRA API this method builds a
        `Parametro`, requests the data and writes a CSV to
        ``data_dir / f"t-{sidra_tabela}" / filename``. If a destination
        file already exists it is skipped.

        Args:
            sidra_tabela: SIDRA table code (numeric string accepted).
            territories: Mapping of territory type codes to lists of
                territory identifiers (e.g. {"6": ["1234567"]}).
            variables: Optional list of variable codes to request. If
                omitted, the special value ["all"] is used.
            classifications: Optional mapping of classification id to a
                list of category ids. If omitted the method will fetch
                metadata and default to empty category lists for every
                classification.

        Returns:
            A list of ``pathlib.Path`` objects pointing to the files
            created or found on disk.
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
        for i, periodo in enumerate(periodos):
            if i == 0:
                formato = Formato.A  # Formato: Códigos e Nomes dos descritores
            else:
                formato = Formato.C  # Formato: Apenas códigos dos descritores
            parameter = Parametro(
                agregado=sidra_tabela,
                territorios=territories,
                variaveis=variables,
                periodos=[periodo.id],
                classificacoes=classifications,
                decimais={"": Precisao.M},  # Precisão: Máxima
                formato=formato,
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
            data = self.get_table(parameter)
            write_json(data=data, dest_filepath=dest_filepath)
            filepaths.append(dest_filepath)
        return filepaths

    def get_table(self, parameter: Parametro) -> dict:
        """Request a SIDRA table and return it as a dictionary.

        This method calls the underlying `SidraClient` using the URL
        produced by ``parameter.url()``. It retries on common transient
        network errors (`httpx.ReadTimeout`, `httpx.RemoteProtocolError`),
        sleeping briefly between attempts.

        Args:
            parameter: A `Parametro` instance with the desired request
                configuration.

        Returns:
            A `dict` constructed from the JSON response.
        """
        url = parameter.url()
        while True:
            try:
                data = self.sidra_client.get(url)
                return data
            except httpx.ReadTimeout as e:
                logger.error("Read timeout while fetching data: %s", e)
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)
            except httpx.RemoteProtocolError as e:
                logger.error("Remote protocol error: %s", e)
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)

    def __enter__(self):
        """Enter the context manager and return this `Fetcher`.

        The underlying `SidraClient` does not require explicit startup
        steps here, but this method allows use in ``with`` statements.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close resources held by the fetcher.

        Delegates to the `SidraClient` context manager to ensure any
        network resources are cleaned up. Arguments are forwarded from
        the context manager protocol.
        """
        self.sidra_client.__exit__(exc_type, exc_value, traceback)


def unnest_classificacoes(
    classificacoes: list[Classificacao],
    data: dict[str, list[str]] | None = None,
) -> Generator[dict[str, list[str]], None, None]:
    """Recursively enumerate classification/category combinations.

    SIDRA classifications can be nested. This generator produces a flat
    sequence of mappings suitable to pass as the ``classificacoes``
    parameter when requesting aggregated data: each yielded dict maps a
    classification id (string) to a single-element list containing a
    category id (string).

    The function skips categories with id "0" which usually represent
    an undefined or "all" category.

    Args:
        classificacoes: List of `Classificacao` objects (from
            `sidra_fetcher`) to expand.
        data: Internal accumulator used by recursion; callers should
            normally omit this argument.

    Yields:
        Dictionaries mapping classification id to a singleton list of
        category ids, representing one combination of categories across
        the provided classifications.
    """
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
