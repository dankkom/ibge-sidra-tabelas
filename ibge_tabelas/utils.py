from importlib import resources
from pathlib import Path

import requests
import sqlalchemy as sa

from .config import TMP_DIR, Config

BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"


def get_periodos(agregado):
    url = BASE_URL + "{agregado}/periodos".format(agregado=agregado)
    response = requests.get(url)
    return response.json()


def temp_dir() -> Path:
    tmp = Path(TMP_DIR)
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
