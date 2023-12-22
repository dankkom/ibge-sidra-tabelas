import tempfile
from importlib import resources
from pathlib import Path

import sqlalchemy as sa

from .config import Config


def temp_dir() -> Path:
    tmp = Path(tempfile.gettempdir()) / "ibge_tabelas"
    tmp.mkdir(exist_ok=True)
    return tmp


def load_municipios():
    with resources.open_text("ibge_tabelas", "municipios.txt") as f:
        municipios = [mun.strip() for mun in f.readlines()]
    return municipios
