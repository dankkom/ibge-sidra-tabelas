# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

import logging
from importlib.metadata import version

from . import config, database, sidra, storage

__version__ = version("sidra-sql")

__all__ = ["config", "database", "sidra", "storage"]

logging.getLogger(__name__).addHandler(logging.NullHandler())

config.setup_logging(__name__, "sidra-sql.log")
