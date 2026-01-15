"""Helpers to construct filenames and read/write SIDRA CSV files.

This module centralizes filesystem operations for storing SIDRA tables
under the project's data directory. It exposes utilities to obtain the
base data directory, construct deterministic filenames from a
`Parametro`, and read/write CSV files produced by the SIDRA API so that
downstream modules can rely on a consistent format and logging.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from sidra_fetcher.sidra import Parametro

from .config import DATA_DIR

logger = logging.getLogger(__name__)


def get_data_dir() -> Path:
    """Return the project's data directory for raw IBGE tables.

    The directory is created if it does not already exist. The returned
    path points to ``DATA_DIR / 'raw' / 'ibge-tabelas'``.

    Returns:
        A `pathlib.Path` instance pointing to the data directory.
    """
    data_dir = DATA_DIR / "raw" / "ibge-tabelas"
    data_dir.mkdir(exist_ok=True, parents=True)
    return data_dir


def get_filename(parameter: Parametro, modification: str):
    """Build a deterministic filename for a SIDRA `Parametro`.

    The filename encodes the table id, periods, format, territorial
    levels, variables and classifications so that each unique request
    maps to a unique JSON name. The returned filename ends with
    ``@{modification}.json`` where ``modification`` is typically the
    period modification timestamp used by the API.

    Args:
        parameter: A `Parametro` instance containing request
            configuration (table, periods, territorios, variaveis,
            classificacoes, formato).
        modification: A string representing the modification timestamp
            to append to the filename.

    Returns:
        A string suitable for use as a JSON filename.
    """
    sidra_table = parameter.agregado
    periods = ",".join(parameter.periodos)
    formato = parameter.formato.value
    name = f"t-{sidra_table}_p-{periods}_f-{formato}"
    for territorial_level in parameter.territorios:
        territorial_codes = ",".join(
            str(code) for code in parameter.territorios[territorial_level]
        )
        if territorial_codes == "":
            territorial_codes = "all"
        name += f"_n{territorial_level}-{territorial_codes}"
    if parameter.variaveis:
        variables = ",".join(str(var) for var in parameter.variaveis)
        name += f"_v-{variables}"
    for classification, categories in parameter.classificacoes.items():
        str_categories = ",".join(categories)
        name += f"_c{classification}-{str_categories}"
    name += f"@{modification}"
    name += ".json"
    return name


def write_json(data: dict, dest_filepath: Path):
    """Write a dictionary to JSON using UTF-8 encoding.

    The function logs the operation and writes the provided dictionary to
    ``dest_filepath`` with ``index=False`` to match the format produced
    by the SIDRA API client.

    Args:
        data: A `dict` containing the table data.
        dest_filepath: Destination `pathlib.Path` where the JSON will be
            written. Parent directories should already exist.
    """
    logger.info("Writing file %s", dest_filepath)
    with dest_filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f)


def write_file(df: pd.DataFrame, dest_filepath: Path):
    """Write a DataFrame to CSV using UTF-8 encoding.

    The function logs the operation and writes the provided DataFrame to
    ``dest_filepath`` with ``index=False`` to match the format produced
    by the SIDRA API client.

    Args:
        df: A `pandas.DataFrame` containing the table data.
        dest_filepath: Destination `pathlib.Path` where the CSV will be
            written. Parent directories should already exist.
    """
    logger.info("Writing file %s", dest_filepath)
    df.to_csv(dest_filepath, index=False, encoding="utf-8")


def read_json(filepath: Path) -> pd.DataFrame:
    """Read a JSON file previously written by `write_json`.

    Args:
        filepath: Path to the JSON file to read.

    Returns:
        A `pandas.DataFrame` containing the table data.
    """
    logger.info("Reading file %s", filepath)
    with filepath.open("r", encoding="utf-8") as f:
        # Skip the first row
        next(f)
        return pd.read_json(f, orient="records", lines=True, na_values=["...", "-"])


def read_file(filepath: Path, **read_csv_args: Any) -> pd.DataFrame:
    """Read a SIDRA CSV file previously written by `write_file`.

    This helper applies the conventions used when writing files:
    - Skips the first row which sometimes contains metadata.
    - Treats the strings "..." and "-" as missing values.
    - Drops rows where the ``Valor`` column is missing.

    Any additional keyword arguments are forwarded to
    ``pandas.read_csv``.

    Args:
        filepath: Path to the CSV file to read.
        **read_csv_args: Additional arguments passed to
            ``pandas.read_csv``.

    Returns:
        A `pandas.DataFrame` with cleaned table rows.
    """
    logger.info("Reading file %s", filepath)
    data = pd.read_csv(filepath, skiprows=1, na_values=["...", "-"], **read_csv_args)
    data = data.dropna(subset="Valor")
    return data
