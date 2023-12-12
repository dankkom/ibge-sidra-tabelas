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


def get_engine(config: Config) -> sa.engine.Engine:
    db_user = config.db_user
    db_password = config.db_password
    db_host = config.db_host
    db_port = config.db_port
    db_name = config.db_name
    connection_string = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = sa.create_engine(connection_string)
    return engine
