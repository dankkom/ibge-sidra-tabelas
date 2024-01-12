from pathlib import Path

import requests
import sidrapy

from .utils import get_periodos, temp_dir

BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"


def get_periodos(agregado):
    url = BASE_URL + "{agregado}/periodos".format(agregado=agregado)
    response = requests.get(url)
    return response.json()


def get_localidades(agregado, nivel):
    url = BASE_URL + "{agregado}/localidades/{nivel}".format(
        agregado=agregado,
        nivel=nivel,
    )
    response = requests.get(url)
    return response.json()


def download_table(
    sidra_tabela: str,
    territorial_level: str,
    ibge_territorial_code: str,
    variable: str = None,
    classifications: dict = None,
    categories: str = None,
) -> list[Path]:
    """Download a SIDRA table in CSV format on temp_dir()

    Args:
        sidra_tabela (str): SIDRA table code
        territorial_level (str): territorial level code
        ibge_territorial_code (str): IBGE territorial code
        variable (str, optional): variable code. Defaults to None.
        classifications (dict, optional): classifications codes. Defaults to None.
        categories (str, optional): categories codes. Defaults to None.

    Returns:
        list[Path]: list of downloaded files
    """
    filepaths = []
    periodos = get_periodos(sidra_tabela)
    for periodo in periodos:
        dest_filepath = temp_dir() / f"{sidra_tabela}-{periodo['id']}.csv"
        if dest_filepath.exists():
            continue
        print(f"Downloading {sidra_tabela}-{periodo['id']}")
        df = sidrapy.get_table(
            table_code=sidra_tabela,  # Tabela SIDRA
            territorial_level=territorial_level,  # Nível de Municípios
            ibge_territorial_code=ibge_territorial_code,  # Territórios
            period=periodo["id"],  # Período
            variable=variable,  # Variáveis
            classifications=classifications,
            categories=categories,
        )
        df.to_csv(dest_filepath, index=False, encoding="utf-8")
        filepaths.append(dest_filepath)
    return filepaths
