# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of ibge-sidra-tabelas.
#
# ibge-sidra-tabelas is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ibge-sidra-tabelas is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ibge-sidra-tabelas.  If not, see <https://www.gnu.org/licenses/>.

"""CLI entry point for running TOML-defined SIDRA data-loading scripts.

Usage::

    python scripts/run.py scripts/pibmunic.toml
    python scripts/run.py scripts/snpc/ipca.toml
"""

import argparse
import logging
from pathlib import Path

from ibge_sidra_tabelas.config import Config
from ibge_sidra_tabelas.toml_runner import TomlScript


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run a SIDRA data-loading pipeline from a TOML config file"
    )
    parser.add_argument(
        "toml_file",
        type=Path,
        help="Path to the TOML file defining the tables to fetch",
    )
    args = parser.parse_args()

    config = Config()
    script = TomlScript(config, args.toml_file)
    script.run()


if __name__ == "__main__":
    main()
