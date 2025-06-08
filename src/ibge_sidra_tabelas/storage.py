import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .config import DATA_DIR

logger = logging.getLogger(__name__)


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
    data_modificacao: str = None,
):
    name = f"t-{sidra_tabela}_p-{periodo}"
    name += f"_n{territorial_level}-{ibge_territorial_code}"
    name += f"_v-{variable}"
    if classifications is not None:
        for classificacao, categoria in classifications.items():
            name += f"_c{classificacao}-{categoria}"
    name += f"@{data_modificacao}" if data_modificacao is not None else ""
    name += ".csv"
    return name


def write_file(df: pd.DataFrame, dest_filepath: Path):
    logger.info("Writing file %s", dest_filepath)
    df.to_csv(dest_filepath, index=False, encoding="utf-8")


def read_file(filepath: Path, **read_csv_args: Any) -> pd.DataFrame:
    logger.info("Reading file %s", filepath)
    data = pd.read_csv(filepath, skiprows=1, na_values=["...", "-"], **read_csv_args)
    data = data.dropna(subset="Valor")
    return data
