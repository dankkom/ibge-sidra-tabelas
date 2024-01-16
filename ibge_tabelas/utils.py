import tempfile
from importlib import resources
from pathlib import Path


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
