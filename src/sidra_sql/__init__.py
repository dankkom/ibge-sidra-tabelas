import logging
from importlib.metadata import PackageNotFoundError, version

from . import config, database, sidra, storage

try:
    __version__ = version("sidra-sql")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["config", "database", "sidra", "storage"]

logging.getLogger(__name__).addHandler(logging.NullHandler())

config.setup_logging(__name__, "sidra-sql.log")
