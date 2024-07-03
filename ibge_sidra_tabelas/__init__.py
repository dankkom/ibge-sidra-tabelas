import logging

from . import config, database, sidra, storage


logging.getLogger(__name__).addHandler(logging.NullHandler())

config.setup_logging(__name__, "ibge-sidra-tabelas.log")
