import configparser
import os
from pathlib import Path

import requests

BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"


class Config:
    def __init__(self, db_name):
        config = configparser.ConfigParser()
        config_dir = Path(os.getenv("CONFIG_DIR"))
        config_filepath = config_dir / "dataengineering" / "db.ini"
        config.read(config_filepath)
        self.db_user = config[db_name]["user"]
        self.db_password = config[db_name]["password"]
        self.db_host = config[db_name]["host"]
        self.db_port = config[db_name]["port"]


def get_periodos(agregado):
    url = BASE_URL + "{agregado}/periodos".format(agregado=agregado)
    response = requests.get(url)
    return response.json()
