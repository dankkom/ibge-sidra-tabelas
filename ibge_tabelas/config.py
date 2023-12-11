import configparser

from pathlib import Path

TMP_DIR = Path("tmp")


class Config:
    def __init__(self, db_table):
        self.config = configparser.ConfigParser()
        self.config.read(Path(__file__).parent / "config.ini")

        self.db_user = self.config["database"]["user"]
        self.db_password = self.config["database"]["password"]
        self.db_host = self.config["database"]["host"]
        self.db_port = self.config["database"]["port"]

        self.db_name = self.config["database"]["name"]
        self.db_schema = self.config["database"]["schema"]
        self.db_table = db_table
        self.db_tablespace = self.config["database"]["tablespace"]
