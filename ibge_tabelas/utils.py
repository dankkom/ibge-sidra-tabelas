import tempfile
from importlib import resources
from pathlib import Path
from typing import Any


def temp_dir() -> Path:
    tmp = Path(tempfile.gettempdir()) / "ibge_tabelas"
    tmp.mkdir(exist_ok=True)
    return tmp


def load_municipios():
    with resources.open_text("ibge_tabelas", "municipios.txt") as f:
        municipios = [mun.strip() for mun in f.readlines()]
    return municipios


def get_filename(
    sidra_tabela: str,
    periodo: str,
    territorial_level: str,
    ibge_territorial_code: str,
    variable: str = "allxp",
):
    name = f"t{sidra_tabela}_p{periodo}"
    name += f"_n{territorial_level}-{ibge_territorial_code}"
    name += f"_v-{variable}"
    name += ".csv"
    return name


def list_classificacoes(
    classificacoes: list[dict],
    data: dict[str, str] = None,
) -> list[dict[str, str]]:
    """Recursively list all classifications and categories"""
    if data is None:
        data = {}
    for i, classificacao in enumerate(classificacoes, 1):
        classificacao_id = classificacao["id"]
        for categoria in classificacao["categorias"]:
            categoria_id = str(categoria["id"])
            if categoria_id == "0":
                continue
            data[f"c{classificacao_id}"] = categoria_id
            if len(classificacoes) == 1:
                yield data
            else:
                yield from list_classificacoes(classificacoes[i:], data)
