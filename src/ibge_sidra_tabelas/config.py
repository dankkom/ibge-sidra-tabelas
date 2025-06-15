import configparser
import logging
import os
from logging import handlers
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))


class Config:
    def __init__(self, db_table: str):
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
        self.db_readonly_role = self.config["database"]["readonly_role"]

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
            f"db_readonly_role: {self.db_readonly_role}\n"
        )


def setup_logging(logger_name: str, log_filepath: Path):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    log_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File log
    filehandler = handlers.RotatingFileHandler(
        filename=log_filepath,
        mode="a",
        maxBytes=50 * 2**20,
        backupCount=100,
    )
    filehandler.setFormatter(log_formatter)
    filehandler.setLevel(logging.INFO)
    logger.addHandler(filehandler)

    # Console log
    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(log_formatter)
    streamhandler.setLevel(logging.DEBUG)
    logger.addHandler(streamhandler)

    return logger
