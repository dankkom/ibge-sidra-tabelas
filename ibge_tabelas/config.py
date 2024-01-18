import configparser
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))


class Config:
    def __init__(self, db_table):
        self.config = configparser.ConfigParser()
        self.config.read(Path("config.ini"))

        self.db_user = self.config["database"]["user"]
        self.db_password = self.config["database"]["password"]
        self.db_host = self.config["database"]["host"]
        self.db_port = self.config["database"]["port"]

        self.db_name = self.config["database"]["name"]
        self.db_schema = self.config["database"]["schema"]
        self.db_table = db_table
        self.db_tablespace = self.config["database"]["tablespace"]

    def __str__(self):
        return (
            f"db_user: {self.db_user}\n"
            f"db_password: {self.db_password}\n"
            f"db_host: {self.db_host}\n"
            f"db_port: {self.db_port}\n"
            f"db_name: {self.db_name}\n"
            f"db_schema: {self.db_schema}\n"
            f"db_table: {self.db_table}\n"
            f"db_tablespace: {self.db_tablespace}\n"
        )
