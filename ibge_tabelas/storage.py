from pathlib import Path

from .config import DATA_DIR


def get_data_dir() -> Path:
    data_dir = DATA_DIR / "raw" / "ibge-tabelas"
    data_dir.mkdir(exist_ok=True, parents=True)
    return data_dir


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