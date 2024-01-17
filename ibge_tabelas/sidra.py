from pathlib import Path

import requests
import sidrapy

from .utils import get_filename, temp_dir

BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"


def get_periodos(agregado: str):
    url = BASE_URL + "{agregado}/periodos".format(agregado=agregado)
    response = requests.get(url)
    return response.json()


def get_localidades(agregado: str, nivel: str) -> list[dict[str, str]]:
    url = BASE_URL + "{agregado}/localidades/{nivel}".format(
        agregado=agregado,
        nivel=nivel,
    )
    response = requests.get(url)
    return response.json()


def get_metadados(agregado: str) -> dict[str, str]:
    url = BASE_URL + "{agregado}/metadados".format(agregado=agregado)
    response = requests.get(url)
    return response.json()


def download_table(
    sidra_tabela: str,
    territorial_level: str,
    ibge_territorial_code: str,
    variable: str = None,
    classifications: dict = None,
) -> list[Path]:
    """Download a SIDRA table in CSV format on temp_dir()

    Args:
        sidra_tabela (str): SIDRA table code
        territorial_level (str): territorial level code
        ibge_territorial_code (str): IBGE territorial code
        variable (str, optional): variable code. Defaults to None.
        classifications (dict, optional): classifications and categories codes. Defaults to None.

    Returns:
        list[Path]: list of downloaded files
    """
    filepaths = []
    periodos = get_periodos(sidra_tabela)
    for periodo in periodos:
        filename = get_filename(
            sidra_tabela=sidra_tabela,
            periodo=periodo["id"],
            territorial_level=territorial_level,
            ibge_territorial_code=ibge_territorial_code,
            variable=variable,
            classifications=classifications,
        )
        dest_filepath = temp_dir() / filename
        if dest_filepath.exists():
            filepaths.append(dest_filepath)
            continue
        print(f"Downloading {filename}")
        df = sidrapy.get_table(
            table_code=sidra_tabela,  # Tabela SIDRA
            territorial_level=territorial_level,  # Nível de Municípios
            ibge_territorial_code=ibge_territorial_code,  # Territórios
            period=periodo["id"],  # Período
            variable=variable,  # Variáveis
            classifications=classifications,
        )
        df.to_csv(dest_filepath, index=False, encoding="utf-8")
        filepaths.append(dest_filepath)
    return filepaths
