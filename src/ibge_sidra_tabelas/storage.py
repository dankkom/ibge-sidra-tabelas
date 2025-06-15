import logging
from pathlib import Path
from typing import Any

import pandas as pd
from sidra_fetcher.api.sidra import Parametro

from .config import DATA_DIR

logger = logging.getLogger(__name__)


def get_data_dir() -> Path:
    data_dir = DATA_DIR / "raw" / "ibge-tabelas"
    data_dir.mkdir(exist_ok=True, parents=True)
    return data_dir


def get_filename(parameter: Parametro, modification: str):
    """Generate a filename for the given parameter.
    Args:
        parameter (Parametro): The parameter containing the table and territorial information.
        modification (str | None): Optional modification string to append to the filename.
    Returns:
        str: The generated filename.
    """
    sidra_table = parameter.agregado
    periods = ",".join(parameter.periodos)
    name = f"t-{sidra_table}_p-{periods}"
    for territorial_level in parameter.territorios:
        territorial_codes = ",".join(
            str(code) for code in parameter.territorios[territorial_level]
        )
        if territorial_codes == "":
            territorial_codes = "all"
        name += f"_n{territorial_level}-{territorial_codes}"
    for variable in parameter.variaveis:
        name += f"_v-{variable}"
    for classification, categories in parameter.classificacoes.items():
        str_categories = ",".join(categories)
        name += f"_c{classification}-{str_categories}"
    name += f"@{modification}"
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
