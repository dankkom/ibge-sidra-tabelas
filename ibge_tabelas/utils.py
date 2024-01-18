from importlib import resources
from pathlib import Path

from .config import DATA_DIR


def get_data_dir() -> Path:
    data_dir = DATA_DIR / "raw" / "ibge-tabelas"
    data_dir.mkdir(exist_ok=True, parents=True)
    return data_dir


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
    classifications: dict[str, str] = None,
):
    name = f"t-{sidra_tabela}_p-{periodo}"
    name += f"_n{territorial_level}-{ibge_territorial_code}"
    name += f"_v-{variable}"
    if classifications is not None:
        for classificacao, categoria in classifications.items():
            name += f"_c{classificacao}-{categoria}"
    name += ".csv"
    return name


def unnest_classificacoes(
    classificacoes: list[dict],
    data: dict[str, str] = None,
) -> dict[str, str]:
    """Recursively list all classifications and categories"""
    if data is None:
        data = {}
    for i, classificacao in enumerate(classificacoes, 1):
        classificacao_id = classificacao["id"]
        for categoria in classificacao["categorias"]:
            categoria_id = str(categoria["id"])
            if categoria_id == "0":
                continue
            data[f"{classificacao_id}"] = categoria_id
            if len(classificacoes) == 1:
                yield dict(**data)
            else:
                yield from unnest_classificacoes(classificacoes[i:], data)
